from .models import Messages
def pid_number(pid_d, date_report): 
    latest_question_list = Messages.objects.filter(date=date_report)
    for q in latest_question_list:
        if q.pid_thread not in pid_d:
            pid_d[q.pid_thread] = 1 
        else:
            pid_d[q.pid_thread] += 1            
    context = {
        'latest_question_list':  latest_question_list,
        'pid_d': sorted(pid_d.items()),
        'date_report': date_report,
        'sum_obj': sum(pid_d.values()),
    }     
    return pid_d, context