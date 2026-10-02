from croniter import croniter

c = croniter("0 0 * * *")  # every 10s

def get_next_run_time(cron_expression, start_time):
    c = croniter(cron_expression, start_time)
    return c.get_next()

