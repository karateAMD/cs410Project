from django.conf.urls import include, url
from django.contrib import admin
from . import views

urlpatterns = [
    url(r'^$', views.home),
    url(r'^search1$', views.treemap1),
    url(r'^search2$', views.treemap2),
    url(r'^search3$', views.treemap3),
    url(r'^search4$', views.treemap4),
]
