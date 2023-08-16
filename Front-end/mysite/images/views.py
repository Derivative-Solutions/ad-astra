from django.shortcuts import render
from .fetch_images import get_apod

# Create your views here.
def index(request):

    apod_data = get_apod("ad-astra-bucket")

    return render(request, "images/index.html", {
        'title': apod_data['title'],
        'copyright': apod_data['copyright'],
        'date': apod_data['date'],
        'explanation': apod_data['explanation'],
        "url": apod_data["url"]
    })