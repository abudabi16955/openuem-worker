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
