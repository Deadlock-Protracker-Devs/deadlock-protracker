from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.decorators import action
from rest_framework.response import Response
from django.utils.timezone import now
from datetime import timedelta
from .models import (
    Match,
    Hero,
    ShopItem,
    Account,
    PlayerPerformance,
)
from .serializers import (
    MatchSerializer,
    HeroSerializer,
    ShopItemSerializer,
    PlayerSerializer,
)

class MatchViewSet(ReadOnlyModelViewSet):
    """
    GET /api/tracker/matches/
    GET /api/tracker/matches/<id>/
    GET /api/tracker/matches/?range=30d
    """
    serializer_class = MatchSerializer

    def get_queryset(self):
        qs = Match.objects.all().order_by("-date")

        range_param = self.request.query_params.get("range")
        if range_param == "30d":
            qs = qs.filter(date__gte=now() - timedelta(days=30))

        return qs
    
    @action(detail=True, methods=["get"])
    def players(self, request, pk=None):
        """
        GET /api/tracker/matches/<match_id>/players/
        """
        performances = PlayerPerformance.objects.filter(match_id=pk)

        data = [
            {
                "account_id": p.account_id.account_id,
                "username": p.account_id.username,
                "kills": p.kills,
                "deaths": p.deaths,
                "assists": p.assists,
                "networth": p.networth,
                "team": p.team,
                "is_win": p.is_win,
            }
            for p in performances
        ]

        return Response(data)

class HeroViewSet(ReadOnlyModelViewSet):
    """
    GET /api/tracker/heroes/
    GET /api/tracker/heroes/<id>/
    """
    queryset = Hero.objects.all()
    serializer_class = HeroSerializer

class ItemViewSet(ReadOnlyModelViewSet):
    """
    GET /api/tracker/items/
    GET /api/tracker/items/<id>/
    """
    queryset = ShopItem.objects.all()
    serializer_class = ShopItemSerializer
    
class PlayerViewSet(ReadOnlyModelViewSet):
    """
    GET /api/tracker/players/
    GET /api/tracker/players/<id>/
    """
    queryset = Account.objects.all()
    serializer_class = PlayerSerializer

    @action(detail=True, methods=["get"])
    def matches(self, request, pk=None):
        """
        GET /api/tracker/players/<account_id>/matches/
        """
        performances = PlayerPerformance.objects.filter(account_id=pk)

        data = [
            {
                "match_id": p.match_id.match_id,
                "kills": p.kills,
                "deaths": p.deaths,
                "assists": p.assists,
                "is_win": p.is_win,
            }
            for p in performances
        ]

        return Response(data)

