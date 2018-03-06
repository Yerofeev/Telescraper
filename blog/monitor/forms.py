from django import forms
from monitor.models import Messages
from datetime import date

class DateRangeForm(forms.ModelForm):
    #BIRTH_YEAR_CHOICES = ('2018','2017','2016')
    #name = forms.CharField(max_length=128, help_text="Please enter the category name.")
    #name = forms.DateField(widget=forms.SelectDateWidget())
    day = date.today()
    date = forms.DateField(input_formats=['%Y-%m-%d'],required=True, widget=forms.SelectDateWidget(), initial=date.today())    
    #views = forms.IntegerField(widget=forms.HiddenInput(), initial=0)
    #likes = forms.IntegerField(widget=forms.HiddenInput(), initial=0)

    # An inline class to provide additional information on the form.
    class Meta:
        # Provide an association between the ModelForm and a model
        model = Messages
        fields = ('date',) 
        widgets = {'date': forms.DateInput(format='%Y-%m-%d', attrs={'class': 'datepicker'})}
        