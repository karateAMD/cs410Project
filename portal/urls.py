from django.conf.urls import include, url
from django.contrib import admin
from . import views

urlpatterns = [
    url(r'^$', views.home),
    url(r'^search=RomaTheEternalCity$', views.treemap1),
    url(r'^search=LetThemEat$', views.treemap2),
    url(r'^search=TheThirst$', views.treemap3),
    url(r'^search=ManWithXrayEyes$', views.treemap4),
]
