"""
URL configuration for livescore project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

from livescore.views import bundesliga_results_api, csv_view, epl_fixtures_api, eredivisie_results_api, fixtures_api, home, laliga_results_api, ligaportugal_results_api, ligue1_results_api, match_detail, seriea_results_api, superlig_results_api
from livescore.views import bundesliga_results_api, csv_view, epl_fixtures_api, eredivisie_results_api, fixtures_api, home, laliga_results_api, ligaportugal_results_api, ligue1_results_api, match_detail, saudi_results_api, seriea_results_api, superlig_results_api

urlpatterns = [
    path('', home, name='home'),
    path('match/', match_detail, name='match_detail'),
    path('csv/', csv_view, name='csv_view'),
    path('api/fixtures/', fixtures_api, name='fixtures_api'),
    path('api/epl-fixtures/', epl_fixtures_api, name='epl_fixtures_api'),
    path('api/laliga-results/', laliga_results_api, name='laliga_results_api'),
    path('api/bundesliga-results/', bundesliga_results_api, name='bundesliga_results_api'),
    path('api/seriea-results/', seriea_results_api, name='seriea_results_api'),
    path('api/ligue1-results/', ligue1_results_api, name='ligue1_results_api'),
    path('api/eredivisie-results/', eredivisie_results_api, name='eredivisie_results_api'),
    path('api/ligaportugal-results/', ligaportugal_results_api, name='ligaportugal_results_api'),
    path('api/superlig-results/', superlig_results_api, name='superlig_results_api'),
        path('api/saudi-results/', saudi_results_api, name='saudi_results_api'),
    path('admin/', admin.site.urls),
]
