from django.db import models

class Product (models.Model):
    ProdID = models.CharField(max_length=15, primary_key=True)
    #Category = models.CharField(max_length = 40)
    Price = models.DecimalField(decimal_places=2, default=0)
    MeanRating = models.DecimalField(decimal_places=1, default=0)
    ModeRating = models.IntegerField(default=0)
    NumReviews = models.IntegerField(default=0)
    GoodWords = models.JSONField()
    BadWords = models.JSONField()
    Name = models.TextField(null=True)
