from rest_framework.routers import DefaultRouter
from .views import (
    MatchViewSet,
    HeroViewSet,
    ItemViewSet,
    PlayerViewSet,
)

router = DefaultRouter()
router.register("matches", MatchViewSet, basename="match")
router.register("heroes", HeroViewSet, basename="hero")
router.register("items", ItemViewSet, basename="item")
router.register("players", PlayerViewSet, basename="player")

urlpatterns = router.urls
