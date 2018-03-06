from django.urls import path
from django.conf.urls import  include, url
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from . import views


urlpatterns = [
    path('', views.index, name='index'),
    path('about/', views.about, name='about'),
    path('details/', views.details, name='details'),
    path('datepickerview/', views.datepickerview, name='datepickerview'),
    
]



