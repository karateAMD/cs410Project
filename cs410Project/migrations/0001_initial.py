# -*- coding: utf-8 -*-
# Generated by Django 1.10.6 on 2017-04-23 22:26
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Product',
            fields=[
                ('ProdID', models.CharField(max_length=15, primary_key=True, serialize=False)),
                ('Category', models.CharField(max_length=40)),
                ('Price', models.DecimalField(decimal_places=2, default=0, max_digits=10)),
                ('MeanRating', models.DecimalField(decimal_places=1, default=0, max_digits=2)),
                ('ModeRating', models.IntegerField(default=0)),
                ('NumReviews', models.IntegerField(default=0)),
                ('GoodWords', django.contrib.postgres.fields.jsonb.JSONField()),
                ('BadWords', django.contrib.postgres.fields.jsonb.JSONField()),
                ('Name', models.TextField(null=True)),
            ],
        ),
    ]