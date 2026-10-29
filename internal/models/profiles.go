package models

import (
	"fmt"
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/open-uem/ent"
	"github.com/open-uem/ent/agent"
	"github.com/open-uem/ent/profile"
	"github.com/open-uem/ent/profileissue"
	"github.com/open-uem/ent/site"
	"github.com/open-uem/ent/tag"
	"github.com/open-uem/ent/task"
	"github.com/open-uem/ent/taskreport"
	"github.com/open-uem/ent/tenant"
	"github.com/open-uem/nats"
)

func (m *Model) GetProfilesAppliedToAll(siteID int, tenantID int) ([]*ent.Profile, error) {
	return m.Client.Profile.Query().WithTasks().
		Where(
			profile.DisabledEQ(false),
			profile.ApplyToAll(true),
			profile.Or(
				profile.And(profile.Not(profile.HasTenant()), profile.Not(profile.HasSite())),
				profile.HasTenantWith(tenant.ID(tenantID)),
				profile.HasSiteWith(site.ID(siteID)),
			),
		).All(context.Background())
}

func (m *Model) GetProfilesAppliedToAllFilteredByProfile(siteID int, profileID int) ([]*ent.Profile, error) {
	return m.Client.Profile.Query().WithTasks().Where(
		profile.ID(profileID),
		profile.DisabledEQ(false),
		profile.ApplyToAll(true),
	).All(context.Background())
}

func (m *Model) GetProfilesAppliedToAgent(siteID int, agentID string, tenantID int) ([]*ent.Profile, error) {
	agent, err := m.Client.Agent.Query().WithTags().Where(agent.ID(agentID), agent.HasSiteWith(site.ID(siteID))).Only(context.Background())
	if err != nil {
		return nil, err
	}

	if agent.Edges.Tags != nil {
		tags := []int{}

		for _, tag := range agent.Edges.Tags {
			tags = append(tags, tag.ID)
		}

		return m.Client.Profile.Query().WithTasks().Where(
			profile.DisabledEQ(false),
			profile.HasTagsWith(tag.IDIn(tags...)),
			profile.Or(
				profile.HasSiteWith(site.ID(siteID)),
				profile.HasTenantWith(tenant.ID(tenantID)),
				profile.And(profile.Not(profile.HasTenant()), profile.Not(profile.HasSite())),
			),
		).All(context.Background())
	}

	return []*ent.Profile{}, nil
}

func (m *Model) GetProfilesAppliedToAgentFilteredByProfile(siteID int, agentID string, profileID int) ([]*ent.Profile, error) {
	agent, err := m.Client.Agent.Query().WithTags().Where(agent.ID(agentID), agent.HasSiteWith(site.ID(siteID))).Only(context.Background())
	if err != nil {
		return nil, err
	}

	if agent.Edges.Tags != nil {
		tags := []int{}

		for _, tag := range agent.Edges.Tags {
			tags = append(tags, tag.ID)
		}

		return m.Client.Profile.Query().WithTasks().Where(profile.ID(profileID), profile.DisabledEQ(false), profile.HasTagsWith(tag.IDIn(tags...))).All(context.Background())
	}

	return []*ent.Profile{}, nil
}

func (m *Model) SaveTaskStartReport(report nats.TaskStartReport) error {
	var err error

	exists := true
	profileIssueID := -1
	// Create issue or update the issue
	profileIssue, err := m.Client.ProfileIssue.Query().Where(profileissue.HasProfileWith(profile.ID(report.ProfileID)), profileissue.HasAgentsWith(agent.ID(report.AgentID))).Only(context.Background())
	if err != nil {
		if !ent.IsNotFound(err) {
			return err
		}
		exists = false
	} else {
		profileIssueID = profileIssue.ID
	}

	if !exists {
		profileIssue, err = m.Client.ProfileIssue.Create().SetAgentsID(report.AgentID).SetProfileID(report.ProfileID).Save(context.Background())
		if err != nil {
			return err
		}
		profileIssueID = profileIssue.ID
	}

	var taskIDs []int
	if report.TaskID != 0 {
		taskIDs = append(taskIDs, report.TaskID)
	} else if strings.HasPrefix(report.Name, "task_") {
		taskID, err := strconv.Atoi(strings.Split(strings.TrimPrefix(report.Name, "task_"), "_")[0])
		if err != nil || taskID <= 0 {
			log.Printf("[ERROR]: invalid task name %s in start report for profile %d", report.Name, report.ProfileID)
			return fmt.Errorf("invalid task name format: %s", report.Name)
		}
		taskIDs = append(taskIDs, taskID)
	} else if report.TaskID == 0 && (strings.HasPrefix(report.Name, "profile_") || report.Name == "") {
		// Explicit Unix profile-level start report: mark associated active profile tasks in DB as running with StartedAt
		profileTasks, err := m.Client.Task.Query().
			Where(task.HasProfileWith(profile.ID(report.ProfileID)), task.Disabled(false)).
			All(context.Background())
		if err != nil {
			log.Printf("[ERROR]: could not get profile tasks for start report, reason: %v", err)
			return err
		}
		for _, pt := range profileTasks {
			taskIDs = append(taskIDs, pt.ID)
		}
	} else {
		log.Printf("[ERROR]: unrecognized task start report format (taskID=%d, name=%s) for profile %d", report.TaskID, report.Name, report.ProfileID)
		return fmt.Errorf("unrecognized task start report format: taskID=%d, name=%s", report.TaskID, report.Name)
	}

	for _, taskID := range taskIDs {
		existing, err := m.Client.TaskReport.Query().
			Where(
				taskreport.HasProfileissueWith(profileissue.ID(profileIssueID)),
				taskreport.HasTaskWith(task.ID(taskID)),
			).
			Only(context.Background())

		if err != nil && !ent.IsNotFound(err) {
			log.Printf("[ERROR]: could not check if task report exists, reason: %v", err)
			return err
		}

		if existing != nil {
			// Late start guard: if report arrives after task completion
			// (already completed), do not regress terminal status to running.
			if existing.Status == taskreport.StatusCompleted {
				continue
			}

			// Terminal state cleanup on retry (or update running attempt)
			err := m.Client.TaskReport.Update().
				Where(
					taskreport.HasProfileissueWith(profileissue.ID(profileIssueID)),
					taskreport.HasTaskWith(task.ID(taskID)),
				).
				SetStartedAt(report.StartTime).
				SetStatus(taskreport.StatusRunning).
				SetFailed(false).
				SetTimeout(false).
				SetEnd("").
				SetStdOutput("").
				SetStdError("").
				Exec(context.Background())
			if err != nil {
				log.Printf("[ERROR]: could not update task %d report for profile %d, reason: %v", taskID, profileIssueID, err)
				return err
			}
		} else {
			err := m.Client.TaskReport.Create().
				SetProfileissueID(profileIssueID).
				SetTaskID(taskID).
				SetStartedAt(report.StartTime).
				SetStatus(taskreport.StatusRunning).
				Exec(context.Background())
			if err != nil {
				log.Printf("[ERROR]: could not save task %d report for profile %d, reason: %v", taskID, profileIssueID, err)
				return err
			}
		}
	}

	return nil
}

func (m *Model) SaveProfileApplicationIssues(p nats.ProfileReport) error {
	var err error

	exists := true
	profileIssueID := -1
	// Create issue or update the issue
	profileIssue, err := m.Client.ProfileIssue.Query().Where(profileissue.HasProfileWith(profile.ID(p.ProfileID)), profileissue.HasAgentsWith(agent.ID(p.AgentID))).Only(context.Background())
	if err != nil {
		if !ent.IsNotFound(err) {
			return err
		}
		exists = false
	} else {
		profileIssueID = profileIssue.ID
	}

	if exists {
		if err := m.Client.ProfileIssue.Update().Where(profileissue.ID(profileIssueID)).SetError(p.Error).Exec(context.Background()); err != nil {
			return err
		}
	} else {
		profileIssue, err = m.Client.ProfileIssue.Create().SetError(p.Error).SetAgentsID(p.AgentID).SetProfileID(p.ProfileID).Save(context.Background())
		if err != nil {
			return err
		}
		profileIssueID = profileIssue.ID
	}

	// Now, we must store the report for the profile's tasks
	if p.Tasks != nil {
		for _, report := range p.Tasks {
			var targetTaskIDs []int
			if strings.HasPrefix(report.Name, "profile_") {
				profileTasks, err := m.Client.Task.Query().
					Where(task.HasProfileWith(profile.ID(p.ProfileID)), task.Disabled(false)).
					All(context.Background())
				if err != nil {
					log.Printf("[ERROR]: could not get profile tasks for fallback report, reason: %v", err)
					return err
				}
				for _, pt := range profileTasks {
					targetTaskIDs = append(targetTaskIDs, pt.ID)
				}
			} else {
				taskID, err := strconv.Atoi(strings.Split(strings.TrimPrefix(report.Name, "task_"), "_")[0])
				if err != nil {
					return errors.New("could not convert the task ID from string to int")
				}
				targetTaskIDs = append(targetTaskIDs, taskID)
			}

			for _, taskID := range targetTaskIDs {
				taskExist := true

				theTask, err := m.Client.Task.Query().Where(task.ID(taskID)).First(context.Background())
				if err != nil {
					if ent.IsNotFound(err) {
						taskExist = false
					} else {
						log.Printf("[ERROR]: could not check if profile task exists, reason: %v", err)
						return err
					}
				}

				if !taskExist {
					log.Printf("[ERROR]: the task %d for profile %d, doesn't exist", taskID, profileIssueID)
					continue
				}

				// if we have a task to install, update or delete a package we must try to update the deployment info
				if theTask.Type == task.TypeFlatpakInstall || theTask.Type == task.TypeBrewCaskInstall || theTask.Type == task.TypeBrewFormulaInstall {
					m.SetFlatpakOrBrewDeploymentInfo(taskID, p, report, theTask, "install")
				}

				if theTask.Type == task.TypeBrewCaskUpgrade {
					m.SetFlatpakOrBrewDeploymentInfo(taskID, p, report, theTask, "update")
				}

				if theTask.Type == task.TypeFlatpakUninstall || theTask.Type == task.TypeBrewCaskUninstall || theTask.Type == task.TypeBrewFormulaUninstall {
					m.SetFlatpakOrBrewDeploymentInfo(taskID, p, report, theTask, "uninstall")
				}

				exists, err := m.Client.TaskReport.Query().
					Where(
						taskreport.HasProfileissueWith(profileissue.ID(profileIssueID)),
						taskreport.HasTaskWith(task.ID(taskID)),
					).
					Exist(context.Background())

				if err != nil {
					log.Printf("[ERROR]: could not check if profile issue exists, reason: %v", err)
					return err
				}

				status := taskreport.StatusCompleted
				if report.Failed {
					status = taskreport.StatusFailed
				}

				if exists {
					update := m.Client.TaskReport.Update().
						Where(
							taskreport.HasProfileissueWith(profileissue.ID(profileIssueID)),
							taskreport.HasTaskWith(task.ID(taskID)),
						).
						SetStdError(report.StdErr).
						SetStdOutput(report.StdOut).
						SetEnd(report.EndTime).
						SetFailed(report.Failed).
						SetStatus(status).
						SetTimeout(report.Timeout)

					if report.StartTime != "" {
						update = update.SetStartedAt(report.StartTime)
					}

					err := update.Exec(context.Background())
					if err != nil {
						log.Printf("[ERROR]: could not save task %d report for profile %d, reason: %v", taskID, profileIssueID, err)
					}
				} else {
					create := m.Client.TaskReport.Create().
						SetProfileissueID(profileIssueID).
						SetTaskID(taskID).
						SetStdError(report.StdErr).
						SetStdOutput(report.StdOut).
						SetEnd(report.EndTime).
						SetFailed(report.Failed).
						SetStatus(status).
						SetTimeout(report.Timeout)

					if report.StartTime != "" {
						create = create.SetStartedAt(report.StartTime)
					}

					err := create.Exec(context.Background())
					if err != nil {
						log.Printf("[ERROR]: could not save task %d report for profile %d, reason: %v", taskID, profileIssueID, err)
					}
				}
			}
		}
	}

	return nil
}

func (m *Model) SetFlatpakOrBrewDeploymentInfo(taskID int, p nats.ProfileReport, report nats.TaskReport, t *ent.Task, action string) {
	deployAction := nats.DeployAction{
		Failed:          report.Failed,
		PackageId:       t.PackageID,
		PackageName:     t.PackageName,
		AgentId:         p.AgentID,
		Action:          action,
		PackageBranch:   t.PackageBranch,
		PackageBrewType: t.PackageBrewType,
		PackageVersion:  "",
	}

	if t.Type == task.TypeBrewCaskInstall || t.Type == task.TypeBrewCaskUpgrade || t.Type == task.TypeBrewCaskUninstall {
		deployAction.PackageId = "cask-" + t.PackageID
	}

	when, err := time.Parse(time.RFC3339Nano, report.EndTime)
	if err == nil {
		deployAction.When = when
	}

	if report.StdErr != "" {
		deployAction.Info = report.StdErr
	}

	if err := m.SaveFlatpakOrBrewDeployInfo(deployAction); err != nil {
		log.Printf("[ERROR]: could not save deployment action for flatpak install task %d, reason: %v", taskID, err)
	}
}
