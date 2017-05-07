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

def treemap2(request):
    return render_to_response('treemap2.html')

def treemap3(request):
    return render_to_response('treemap3.html')

def treemap4(request):
    return render_to_response('treemap4.html')
