from django.http import HttpResponse
from django.core.exceptions import ObjectDoesNotExist
import json, os
import cs410Project.models as models

def index(request):
    return HttpResponse("Hello, world. You're at the polls index.")







path = os.path.realpath('./data/stats_amazon_prime_video')
# path = os.path.realpath('./data/tfidf_amazon-instant-video')

def populate_db():
    files = os.listdir(path)
    # for x in range(0,5):
    with open(path+'/'+files[0]) as f:
        data = f.read().replace('\n', '')
        data = '['+data+']'
        product_data = json.loads(data)
        # print(product_data[0]['productID'])
        for prod in product_data:
            try:
                print("1")
                obj = models.Product.objects.get(ProdID=prod['productID'])
                obj.MeanRating = prod['meanRating']
                obj.NumReviews = prod['numReviews']
                obj.ReviewsPerLevel = json.dumps(prod['numReviewsPerRatingLevel'])
                obj.save()
            except ObjectDoesNotExist:
                continue
            # models.Product.objects.get_or_create(
            #     ProdID=prod['productID'],
            #     GoodWords=json.dumps(prod['goodRatingWords']),
            #     BadWords=json.dumps(prod['badRatingWords'])
            #     )
    return

# populate_db()