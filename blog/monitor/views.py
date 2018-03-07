from django.http import HttpResponse
from django.shortcuts import render
import collections
import re
from .models import Messages
from django.template import loader
from calendar import monthrange
from datetime import datetime, date, timedelta
from django.shortcuts import render
from django.http import HttpResponse
from django.template import Context
from django.http import HttpResponseRedirect
from .forms import DateRangeForm
from django.template import RequestContext
from .funcs import pid_number
from time import time
from django.db.models import Count

def about(request):
    if request.method=='GET':
        date_report = request.GET.get('date')    
    #latest_question_list = Messages.objects.order_by('time')[:14]
    #c = Messages.objects.count()
    #output = ', '.join([str(q.date) for q in latest_question_list])    
    #return HttpResponse(output + ' '+ str(c))
    pid_d = {}
    template = loader.get_template('monitor/about.html')    
    latest_question_list = Messages.objects.filter(date=date_report) 
    number_messages = Messages.objects.filter(channel_name='').count()
    #output = ', '.join([str(q.date) for q in latest_question_list])      
    context = {
        'latest_question_list':  latest_question_list,
        'number': number_messages,      
    }        
    return HttpResponse(template.render(context, request))
    
def details(request):
    date_today = datetime.today()
    no_messages_channels = 0
    template = loader.get_template('monitor/details.html')   
    time_24_hours_ago = datetime.now() - timedelta(days=1)
    s = Messages.objects.values('channel_name').distinct().count()
    no_messages_channels = s - Messages.objects.values('channel_name').exclude(message='').distinct().count()
    latest_message = Messages.objects.values('channel_name').distinct().filter(date=date.today().strftime('%Y-%m-%d')).count()#(time__gte=time_24_hours_ago)   
    context = {
        's' :  s,
        'date_report': date_today, 
        'no_messages_channels' : no_messages_channels,
        'number_with_messages_24h': latest_message,
        'number_without_messages_24h' : s - latest_message,
    }          
    return HttpResponse(template.render(context, request))


def index(request):
    latest_question_list = Messages.objects.order_by('time')[:10]
    template = loader.get_template('monitor/index.html')
    context = {
        'latest_question_list': latest_question_list,
    }
    form = DateRangeForm()    
    return HttpResponse(template.render(context, request))

 
def datepickerview(request):
    pid_d = {}
    template = loader.get_template('monitor/datepickerview.html') 
    # A HTTP POST?
    if request.method == 'POST':
        form = DateRangeForm(request.POST)

        # Have we been provided with a valid form?
        if form.is_valid():
            #mydate = form.cleaned_data('date')
            #s.filter(uploaded_time__year=selected_date.year,
            #                             uploaded_time__month=selected_date.month)  
            date_report  = request.POST['date']
            pid_d, context = pid_number(pid_d, date_report)    
            return HttpResponse(template.render(context, request))
        else:
            # The supplied form contained errors - just print them to the terminal.
            print (form.errors)
    else:
        # If the request was not a POST, display the form to enter details.
        date_report  = date.today().strftime('%Y-%m-%d')
        pid_d, context = pid_number(pid_d, date_report)
    # Bad form (or form details), no form supplied...
    # Render the form with error messages (if any).
    #context = RequestContext(request)
    return HttpResponse(template.render(context, request))