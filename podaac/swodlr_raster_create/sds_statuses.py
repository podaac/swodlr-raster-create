'''SDS job statuses grouped as sets'''
SUCCESS = {'job-completed'}
FAIL = {'job-failed', 'job-offline', 'job-deduped'}
WAITING = {'job-queued', 'job-started'}
