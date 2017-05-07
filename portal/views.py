from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response, redirect
from django.template import RequestContext

def home(request):
    base_url = request.build_absolute_uri('/')[:-1]
    return render_to_response('home.html', {
        'base_url': base_url
    })

def treemap1(request):
    return render_to_response('treemap1.html')