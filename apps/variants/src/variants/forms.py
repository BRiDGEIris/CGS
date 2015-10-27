from django import forms

class query_form(forms.Form):
    query_types = ((0, 'CGS - variants'), (1, 'Google Genomics'), (2, 'HBase'), (3, 'Hive'))
    query_type = forms.ChoiceField(choices=query_types)
    query = forms.CharField(min_length=1,max_length=2000, label="Query")
  
  
class query_insert_form(forms.Form):
    samples_ids = forms.CharField(min_length=1,max_length=300000, label="Samples ids")
    import_file = forms.CharField(min_length=1,max_length=500, label="File path")
  
    def __init__(self,*args,**kwargs):
        self.files = kwargs.pop('files')
        super(query_insert_form, self).__init__(*args, **kwargs)
        self.import_file = forms.ChoiceField(choices=self.files)
       
    
    
    
    
    
