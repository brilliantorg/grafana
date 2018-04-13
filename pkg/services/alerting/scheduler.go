package alerting

import (
	"math"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/grafana/grafana/pkg/models"
)

const TICK_HOUR int = 16
const TICK_MINUTE int = 00
const TICK_SECOND int = 00

type SchedulerImpl struct {
	jobs map[int64]*Job
	log  log.Logger
}

func NewScheduler() Scheduler {
	return &SchedulerImpl{
		jobs: make(map[int64]*Job, 0),
		log:  log.New("alerting.scheduler"),
	}
}

func (s *SchedulerImpl) Update(rules []*Rule) {
	s.log.Debug("Scheduling update", "ruleCount", len(rules))

	jobs := make(map[int64]*Job, 0)

	for i, rule := range rules {
		var job *Job
		if s.jobs[rule.Id] != nil {
			job = s.jobs[rule.Id]
		} else {
			job = &Job{
				Running: false,
			}
		}

		job.Rule = rule

		offset := ((rule.Frequency * 1000) / int64(len(rules))) * int64(i)
		job.Offset = int64(math.Floor(float64(offset) / 1000))
		if job.Offset == 0 { //zero offset causes division with 0 panics.
			job.Offset = 1
		}
		jobs[rule.Id] = job
	}

	s.jobs = jobs
}

func (s *SchedulerImpl) Tick(tickTime time.Time, execQueue chan *Job) {
  now := time.Now()
  now_unixtime := now.Unix()
  next_job_queue_unixtime := time.Date(now.Year(), now.Month(), now.Day(), TICK_HOUR, TICK_MINUTE, TICK_SECOND, 00, time.UTC).Unix()

	for _, job := range s.jobs {
    if job.Running || job.Rule.State == models.AlertStatePaused {
			continue
		}
    if next_job_queue_unixtime%now_unixtime == 0 {
        s.enque(job, execQueue)
		}
	}
}

func (s *SchedulerImpl) enque(job *Job, execQueue chan *Job) {
	s.log.Debug("Scheduler: Putting job on to exec queue", "name", job.Rule.Name, "id", job.Rule.Id)
	execQueue <- job
}
