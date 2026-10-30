package models

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/open-uem/ent/enttest"
	"github.com/open-uem/ent/task"
	"github.com/open-uem/ent/taskreport"
	"github.com/open-uem/nats"
)

func TestSaveTaskStartReportValidation(t *testing.T) {
	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()
	m := Model{Client: client}
	ctx := context.Background()

	p, err := client.Profile.Create().SetName("P1").Save(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ag, err := client.Agent.Create().SetID("ag-1").SetHostname("host1").SetOs("linux").Save(ctx)
	if err != nil {
		t.Fatal(err)
	}

	tk1, err := client.Task.Create().SetProfile(p).SetName("Task 1").SetType(task.TypeUnixScript).Save(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Invalid task name (e.g. task_invalid) must return an error and NOT mark all tasks as running
	err = m.SaveTaskStartReport(nats.TaskStartReport{
		ProfileID: p.ID,
		AgentID:   ag.ID,
		TaskID:    0,
		Name:      "task_invalid",
		StartTime: "2026-10-29T10:00:00Z",
	})
	if err == nil {
		t.Fatal("expected error for invalid task name task_invalid")
	}

	reports, err := client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(tk1.ID))).All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Fatalf("expected 0 task reports after invalid task name, got %d", len(reports))
	}

	// 2. Unrecognized name format (not task_, not profile_, not empty) must return an error
	err = m.SaveTaskStartReport(nats.TaskStartReport{
		ProfileID: p.ID,
		AgentID:   ag.ID,
		TaskID:    0,
		Name:      "some_random_report",
		StartTime: "2026-10-29T10:00:00Z",
	})
	if err == nil {
		t.Fatal("expected error for unrecognized report format")
	}

	// 3. Explicit Unix profile-level report (profile_<id>) marks active tasks as running
	err = m.SaveTaskStartReport(nats.TaskStartReport{
		ProfileID: p.ID,
		AgentID:   ag.ID,
		TaskID:    0,
		Name:      "profile_1",
		StartTime: "2026-10-29T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error on profile-level report: %v", err)
	}

	reports, err = client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(tk1.ID))).All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("expected 1 task report, got %d", len(reports))
	}
	if reports[0].Status != taskreport.StatusRunning {
		t.Fatalf("expected status running, got %s", reports[0].Status)
	}

	// 4. Valid single task start report by task_<id>_x
	err = m.SaveTaskStartReport(nats.TaskStartReport{
		ProfileID: p.ID,
		AgentID:   ag.ID,
		TaskID:    0,
		Name:      "task_1_1",
		StartTime: "2026-10-29T10:01:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error on valid task name report: %v", err)
	}
}

func TestSaveTaskStartReportOutOfOrder(t *testing.T) {
	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()
	m := Model{Client: client}
	ctx := context.Background()

	p, err := client.Profile.Create().SetName("P_OOO").Save(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ag, err := client.Agent.Create().SetID("ag-ooo").SetHostname("host-ooo").SetOs("linux").Save(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pi, err := client.ProfileIssue.Create().SetAgentsID(ag.ID).SetProfileID(p.ID).Save(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Case 1: Late start guard for completed task
	t.Run("late_start_after_completed", func(t *testing.T) {
		tk, err := client.Task.Create().SetProfile(p).SetName("Task C").SetType(task.TypeUnixScript).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		_, err = client.TaskReport.Create().
			SetProfileissueID(pi.ID).
			SetTaskID(tk.ID).
			SetStatus(taskreport.StatusCompleted).
			SetStartedAt("2026-10-29T10:00:00Z").
			SetEnd("2026-10-29T10:00:05Z").
			SetStdOutput("success output").
			Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// Late start arrives with StartTime <= End
		err = m.SaveTaskStartReport(nats.TaskStartReport{
			ProfileID: p.ID,
			AgentID:   ag.ID,
			TaskID:    tk.ID,
			StartTime: "2026-10-29T10:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}

		r, err := client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(tk.ID))).Only(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != taskreport.StatusCompleted || r.End != "2026-10-29T10:00:05Z" || r.StdOutput != "success output" {
			t.Fatalf("late start regressed completed report: %+v", r)
		}
	})

	// Case 2: Late start guard for failed / timeout task
	t.Run("late_start_after_failed_or_timeout", func(t *testing.T) {
		tk, err := client.Task.Create().SetProfile(p).SetName("Task F").SetType(task.TypeUnixScript).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		_, err = client.TaskReport.Create().
			SetProfileissueID(pi.ID).
			SetTaskID(tk.ID).
			SetStatus(taskreport.StatusFailed).
			SetFailed(true).
			SetTimeout(true).
			SetStartedAt("2026-10-29T10:00:00Z").
			SetEnd("2026-10-29T10:00:05Z").
			SetStdError("timed out error").
			Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// Late start arrives with StartTime <= End
		err = m.SaveTaskStartReport(nats.TaskStartReport{
			ProfileID: p.ID,
			AgentID:   ag.ID,
			TaskID:    tk.ID,
			StartTime: "2026-10-29T10:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}

		r, err := client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(tk.ID))).Only(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != taskreport.StatusFailed || !r.Timeout || !r.Failed || r.End != "2026-10-29T10:00:05Z" || r.StdError != "timed out error" {
			t.Fatalf("late start regressed failed/timeout report: %+v", r)
		}
	})

	// Case 3: Old start arriving after newer retry
	t.Run("old_start_after_newer_retry", func(t *testing.T) {
		tk, err := client.Task.Create().SetProfile(p).SetName("Task R").SetType(task.TypeUnixScript).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		_, err = client.TaskReport.Create().
			SetProfileissueID(pi.ID).
			SetTaskID(tk.ID).
			SetStatus(taskreport.StatusRunning).
			SetStartedAt("2026-10-29T10:05:00Z").
			Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// Old start arrives with StartTime <= StartedAt
		err = m.SaveTaskStartReport(nats.TaskStartReport{
			ProfileID: p.ID,
			AgentID:   ag.ID,
			TaskID:    tk.ID,
			StartTime: "2026-10-29T10:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}

		r, err := client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(tk.ID))).Only(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != taskreport.StatusRunning || r.StartedAt != "2026-10-29T10:05:00Z" {
			t.Fatalf("old start overwrote newer retry: %+v", r)
		}
	})

	// Case 4: Valid retry after previous timeout / terminal state
	t.Run("valid_retry_after_timeout", func(t *testing.T) {
		tk, err := client.Task.Create().SetProfile(p).SetName("Task Retry").SetType(task.TypeUnixScript).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		_, err = client.TaskReport.Create().
			SetProfileissueID(pi.ID).
			SetTaskID(tk.ID).
			SetStatus(taskreport.StatusFailed).
			SetFailed(true).
			SetTimeout(true).
			SetStartedAt("2026-10-29T10:00:00Z").
			SetEnd("2026-10-29T10:00:05Z").
			SetStdError("timed out error").
			SetStdOutput("partial output").
			Save(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// New retry arrives with StartTime > End
		err = m.SaveTaskStartReport(nats.TaskStartReport{
			ProfileID: p.ID,
			AgentID:   ag.ID,
			TaskID:    tk.ID,
			StartTime: "2026-10-29T10:10:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}

		r, err := client.TaskReport.Query().Where(taskreport.HasTaskWith(task.ID(tk.ID))).Only(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != taskreport.StatusRunning || r.Timeout || r.Failed || r.End != "" || r.StdError != "" || r.StdOutput != "" || r.StartedAt != "2026-10-29T10:10:00Z" {
			t.Fatalf("valid retry did not cleanly reset report: %+v", r)
		}
	})
}
