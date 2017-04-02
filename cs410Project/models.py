from django.db import models

class Product (models.Model):
    ProdID = models.CharField(max_length=15, primary_key=True)
    #Category = models.CharField(max_length = 40)
    Price = models.DecimalField(decimal_places=2)
    MeanRating = models.DecimalField(decimal_places=1)
    ModeRating = models.IntegerField()
    NumReviews = models.IntegerField()
    #words
    Name = models.CharField(max_length=50)
