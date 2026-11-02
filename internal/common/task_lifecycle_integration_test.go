//go:build integration

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/open-uem/ent"
	"github.com/open-uem/ent/task"
	"github.com/open-uem/ent/taskreport"
	wire "github.com/open-uem/nats"
	"github.com/open-uem/openuem-worker/internal/models"
)

// Run only against a disposable database and broker. This test creates schema.
func TestTaskLifecycleIntegration(t *testing.T) {
	dsn, url := os.Getenv("OPENUEM_TEST_DSN"), os.Getenv("OPENUEM_TEST_NATS")
	if dsn == "" || url == "" {
		t.Skip("set OPENUEM_TEST_DSN and OPENUEM_TEST_NATS for isolated infrastructure")
	}
	m, err := models.New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	w := &Worker{Model: m, NATSConnection: nc}
	for subject, handler := range map[string]nats.MsgHandler{"wingetcfg.task_started": w.TaskStartReportResponseHandler, "wingetcfg.report": w.ProfileReportResponseHandler} {
		s, err := nc.QueueSubscribe(subject, "lifecycle-integration", handler)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Unsubscribe()
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	fixture := func(t *testing.T) (*ent.Profile, *ent.Task, string) {
		t.Helper()
		p, err := m.Client.Profile.Create().SetName(t.Name()).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}
		a, err := m.Client.Agent.Create().SetID(fmt.Sprintf("lifecycle-%d", p.ID)).SetHostname("isolated-test").SetOs("linux").Save(ctx)
		if err != nil {
			t.Fatal(err)
		}
		task, err := m.Client.Task.Create().SetProfile(p).SetName("isolated script").SetType(task.TypeUnixScript).SetTimeout(2).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return p, task, a.ID
	}
	send := func(t *testing.T, subject string, v any) {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = nc.Request(subject, b, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	get := func(t *testing.T, id int) *ent.TaskReport {
		t.Helper()
		r, err := m.Client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(id))).Only(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	start := func(p *ent.Profile, task *ent.Task, a string) wire.TaskStartReport {
		return wire.TaskStartReport{ProfileID: p.ID, TaskID: task.ID, AgentID: a, Name: fmt.Sprintf("task_%d_1", task.ID), StartTime: "2026-10-29T06:00:00Z"}
	}
	finish := func(p *ent.Profile, task *ent.Task, a string, failed, timeout bool) wire.ProfileReport {
		return wire.ProfileReport{ProfileID: p.ID, AgentID: a, Timeout: timeout, Tasks: []wire.TaskReport{{Name: fmt.Sprintf("task_%d_1", task.ID), StartTime: "2026-10-29T06:00:00Z", EndTime: "2026-10-29T06:00:02Z", Failed: failed, Timeout: timeout, StdOut: "stdout <script>fixture</script>", StdErr: "stderr fixture"}}}
	}
	for _, tc := range []struct {
		name            string
		failed, timeout bool
	}{{"completed", false, false}, {"failed", true, false}, {"timeout", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			p, task, a := fixture(t)
			if task.Timeout != 2 {
				t.Fatal("configured timeout lost")
			}
			send(t, "wingetcfg.task_started", start(p, task, a))
			r := get(t, task.ID)
			if r.Status != taskreport.StatusRunning || r.StartedAt == "" {
				t.Fatalf("start not persisted: %+v", r)
			}
			send(t, "wingetcfg.report", finish(p, task, a, tc.failed, tc.timeout))
			r = get(t, task.ID)
			want := taskreport.StatusCompleted
			if tc.failed {
				want = taskreport.StatusFailed
			}
			if r.Status != want || r.Timeout != tc.timeout || r.Failed != tc.failed || r.End == "" || r.StartedAt == "" || r.StdOutput != "stdout <script>fixture</script>" || r.StdError != "stderr fixture" {
				t.Fatalf("final state/logs lost: %+v", r)
			}
			// Duplicate final deliveries must not create extra rows.
			send(t, "wingetcfg.report", finish(p, task, a, tc.failed, tc.timeout))
			get(t, task.ID)
		})
	}
	t.Run("legacy_final_without_start", func(t *testing.T) {
		p, task, a := fixture(t)
		v := finish(p, task, a, false, false)
		v.Tasks[0].StartTime = ""
		send(t, "wingetcfg.report", v)
		if get(t, task.ID).Status != taskreport.StatusCompleted {
			t.Fatal("legacy final lost")
		}
	})
	t.Run("task_name_fallback", func(t *testing.T) {
		p, task, a := fixture(t)
		v := start(p, task, a)
		v.TaskID = 0
		send(t, "wingetcfg.task_started", v)
		if get(t, task.ID).Status != taskreport.StatusRunning {
			t.Fatal("name fallback failed")
		}
	})
	t.Run("unix_agent_profile_name", func(t *testing.T) {
		p, task, a := fixture(t)
		v := start(p, task, a)
		v.TaskID = 0
		v.Name = fmt.Sprintf("profile_%d", p.ID)
		send(t, "wingetcfg.task_started", v)
		if get(t, task.ID).Status != taskreport.StatusRunning {
			t.Fatal("Unix start lost")
		}
	})
	t.Run("late_start_must_not_regress_final", func(t *testing.T) {
		p, task, a := fixture(t)
		send(t, "wingetcfg.report", finish(p, task, a, false, false))
		send(t, "wingetcfg.task_started", start(p, task, a))
		if get(t, task.ID).Status != taskreport.StatusCompleted {
			t.Fatal("late start regressed completed task")
		}
	})
	t.Run("late_start_must_not_regress_timeout_or_failed", func(t *testing.T) {
		p, task, a := fixture(t)
		send(t, "wingetcfg.report", finish(p, task, a, true, true))
		send(t, "wingetcfg.task_started", start(p, task, a))
		r := get(t, task.ID)
		if r.Status != taskreport.StatusFailed || !r.Timeout || !r.Failed || r.End == "" {
			t.Fatalf("late start regressed failed/timeout task: %+v", r)
		}
	})
	t.Run("old_start_after_newer_retry_ignored", func(t *testing.T) {
		p, task, a := fixture(t)
		newer := start(p, task, a)
		newer.StartTime = "2026-10-29T06:05:00Z"
		send(t, "wingetcfg.task_started", newer)
		older := start(p, task, a)
		older.StartTime = "2026-10-29T06:00:00Z"
		send(t, "wingetcfg.task_started", older)
		r := get(t, task.ID)
		if r.Status != taskreport.StatusRunning || r.StartedAt != "2026-10-29T06:05:00Z" {
			t.Fatalf("older start overwrote newer retry: %+v", r)
		}
	})
	t.Run("unix_timeout_fallback_report", func(t *testing.T) {
		p, task, a := fixture(t)
		v := finish(p, task, a, true, true)
		v.Tasks[0].Name = fmt.Sprintf("profile_%d", p.ID)
		send(t, "wingetcfg.report", v)
		r := get(t, task.ID)
		if !r.Timeout || !r.Failed {
			t.Fatal("Unix timeout report lost")
		}
	})
	t.Run("generated_windows_resource_start", func(t *testing.T) {
		p, task, a := fixture(t)
		task.Type = "powershell_script"
		task.Script = "Write-Output fixture"
		p.Edges.Tasks = []*ent.Task{task}
		cfg, err := w.GenerateWinGetConfig(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.Properties.Resources) != 1 {
			t.Fatal("expected one resource")
		}
		v := start(p, task, a)
		// Match RunTasks: derive the start identifier from resource.ID, rather
		// than the separate Settings["ID"] used by the script result.
		v.Name = cfg.Properties.Resources[0].ID
		v.TaskID, _ = strconv.Atoi(strings.Split(strings.TrimPrefix(v.Name, "task_"), "_")[0])
		send(t, "wingetcfg.task_started", v)
		if get(t, task.ID).Status != taskreport.StatusRunning {
			t.Fatal("generated resource start lost")
		}
	})
	t.Run("retry_must_clear_previous_timeout", func(t *testing.T) {
		p, task, a := fixture(t)
		send(t, "wingetcfg.report", finish(p, task, a, true, true))
		retry := start(p, task, a)
		retry.StartTime = "2026-10-29T06:05:00Z"
		send(t, "wingetcfg.task_started", retry)
		r := get(t, task.ID)
		if r.Status != taskreport.StatusRunning || r.Failed || r.Timeout || r.End != "" || r.StartedAt != "2026-10-29T06:05:00Z" {
			t.Fatalf("running attempt contains stale terminal fields: %+v", r)
		}
	})
	t.Run("timeout_config_reaches_windows_resource", func(t *testing.T) {
		p, task, a := fixture(t)
		_ = a
		task.Type = "powershell_script"
		task.Script = "Write-Output fixture"
		p.Edges.Tasks = []*ent.Task{task}
		cfg, err := w.GenerateWinGetConfig(p)
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var v any
		if err = json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		var hasTimeout func(any) bool
		hasTimeout = func(v any) bool {
			switch x := v.(type) {
			case map[string]any:
				for k, v := range x {
					if (k == "timeout" || k == "Timeout") && v == float64(2) {
						return true
					}
					if hasTimeout(v) {
						return true
					}
				}
			case []any:
				for _, v := range x {
					if hasTimeout(v) {
						return true
					}
				}
			}
			return false
		}
		if !hasTimeout(v) {
			t.Fatalf("task timeout=2 absent from config: %s", b)
		}
	})
	t.Run("invalid_task_name_must_not_mark_all_running", func(t *testing.T) {
		p, tTask, a := fixture(t)
		v := wire.TaskStartReport{
			ProfileID: p.ID,
			AgentID:   a,
			TaskID:    0,
			Name:      "task_invalid",
			StartTime: "2026-10-29T06:00:00Z",
		}
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = nc.Request("wingetcfg.task_started", b, 5*time.Second)

		reports, err := m.Client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(tTask.ID))).All(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range reports {
			if r.Status == taskreport.StatusRunning {
				t.Fatalf("task was wrongly marked running on invalid task name: %+v", r)
			}
		}
	})
	t.Run("ansible_config_timeout_calculation", func(t *testing.T) {
		p, tTask, a := fixture(t)
		tTask.Type = "unix_script"
		tTask.Script = "echo test"
		p.Edges.Tasks = []*ent.Task{tTask}
		pb, profileTimeout, err := w.GenerateAnsibleConfig(p, a)
		if err != nil {
			t.Fatal(err)
		}
		if pb == nil {
			t.Fatal("expected playbook to be generated")
		}
		if profileTimeout != 2 {
			t.Fatalf("expected profileTimeout 2, got %d", profileTimeout)
		}
	})
}
