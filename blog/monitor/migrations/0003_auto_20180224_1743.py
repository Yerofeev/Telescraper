# Generated by Django 2.0.2 on 2018-02-24 17:43

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('monitor', '0002_auto_20180224_1548'),
    ]

    operations = [
        migrations.DeleteModel(
            name='Category',
        ),
        migrations.RemoveField(
            model_name='entry',
            name='authors',
        ),
        migrations.RemoveField(
            model_name='entry',
            name='blog',
        ),
        migrations.AlterModelOptions(
            name='messages',
            options={'managed': False},
        ),
        migrations.DeleteModel(
            name='Author',
        ),
        migrations.DeleteModel(
            name='Blog',
        ),
        migrations.DeleteModel(
            name='Entry',
        ),
    ]