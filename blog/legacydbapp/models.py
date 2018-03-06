from django.db import models

# Create your models here.
class Messages(models.Model):
    url = models.CharField(max_length=100, blank=True, null=True)
    views = models.IntegerField(blank=True, null=True)
    message = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'messages'