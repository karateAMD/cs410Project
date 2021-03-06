from django.db import models
from django.contrib.postgres.fields import JSONField

class Product (models.Model):
    ProdID = models.CharField(max_length=15, primary_key=True)
    Category = models.CharField(max_length = 40)
    Price = models.DecimalField(decimal_places=2, default=0, max_digits=10)
    MeanRating = models.DecimalField(decimal_places=1, default=0, max_digits=2)
    ModeRating = models.IntegerField(default=0)
    NumReviews = models.IntegerField(default=0)
    GoodWords = models.TextField()
    BadWords = models.TextField()
    ReviewsPerLevel = models.TextField(default='{}')
    Name = models.TextField(default='')


