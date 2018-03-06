from django.db import models
from itertools import groupby
 
class Messages(models.Model):
    channel_name = models.CharField(max_length=255, blank=True, null=True)
    channel_id = models.CharField(max_length=255, blank=True, null=True)
    message_id = models.IntegerField(blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    time = models.TimeField(blank=True, null=True)
    url = models.CharField(max_length=100, blank=True, null=True)
    views = models.IntegerField(blank=True, null=True)
    message = models.TextField(blank=True, null=True)
    date_added = models.DateField(blank=True, null=True)
    time_added = models.TimeField(blank=True, null=True)
    pid_thread = models.CharField(db_column='PID_Thread', max_length=255)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'messages'
       
    def __str__(self):
        return '{},{},{},{}'.format(self.date,self.time,self.message,self.pid_thread)
    
