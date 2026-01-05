from datetime import timedelta

from django.utils.timezone import now, localtime
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from drf_spectacular.utils import (
    extend_schema,
    extend_schema_view,
    OpenApiParameter,
    OpenApiExample,
)
from .models import (
    Match,
    Hero,
    ShopItem,
    Account,
    PlayerPerformance,
    PlayerItem,
    PlayerAbility,
)
from .serializers import (
    MatchSerializer,
    HeroSerializer,
    ShopItemSerializer,
    PlayerSerializer,
)


# ============================================================
# Matches
# ============================================================
@extend_schema_view(
    list=extend_schema(
        summary="List matches",
        description=(
            "Returns matches ordered by most recent first.\n\n"
            "Query params:\n"
            "- `range`: Filter matches by relative date range.\n"
            "  - `30d` = last 30 days\n"
        ),
        parameters=[
            OpenApiParameter(
                name="range",
                type=str,
                required=False,
                description="Filter to a relative date range. Supported: `30d`.",
                examples=[
                    OpenApiExample("Last 30 days", value="30d"),
                ],
            ),
        ],
    ),
    retrieve=extend_schema(
        summary="Retrieve a match",
        description="Return a single match by ID.",
    ),
)
class MatchViewSet(ReadOnlyModelViewSet):
    """
    Endpoints:

    - GET /api/tracker/matches/
      - Optional query params:
        - ?range=30d

    - GET /api/tracker/matches/<id>/

    Extra actions:

    - GET /api/tracker/matches/<match_id>/players/
    - GET /api/tracker/matches/<match_id>/notable_players/
    - GET /api/tracker/matches/<match_id>/events/?account_id=<int>&notable_only=true|false
    - GET /api/tracker/matches/stats/
    """
    queryset = Match.objects.all().order_by("-date")
    serializer_class = MatchSerializer

    def get_queryset(self):
        qs = super().get_queryset()

        range_param = self.request.query_params.get("range")
        if range_param == "30d":
            qs = qs.filter(date__gte=now() - timedelta(days=30))

        return qs

    @extend_schema(
        summary="List players for a match",
        description="Returns all PlayerPerformance rows for this match as a compact list.",
    )
    @action(detail=True, methods=["get"])
    def players(self, request, pk=None):
        """
        GET /api/tracker/matches/<match_id>/players/
        """
        performances = (
            PlayerPerformance.objects
            .filter(match_id=pk)
            .select_related("account_id")
        )

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

    @extend_schema(
        summary="Quick ingestion stats",
        description="Quick counts useful for testing ingestion and data population.",
    )
    @action(detail=False, methods=["get"])
    def stats(self, request):
        """
        GET /api/tracker/matches/stats/
        """
        data = {
            "matches": Match.objects.count(),
            "accounts": Account.objects.count(),
            "player_performances": PlayerPerformance.objects.count(),
            "player_items": PlayerItem.objects.count(),
            "player_abilities": PlayerAbility.objects.count(),
        }
        return Response(data)

    @extend_schema(
        summary="List notable players for a match",
        description="Returns PlayerPerformance rows for notable players only (Account.is_notable=True).",
    )
    @action(detail=True, methods=["get"])
    def notable_players(self, request, pk=None):
        """
        GET /api/tracker/matches/<match_id>/notable_players/
        """
        performances = (
            PlayerPerformance.objects
            .filter(match_id=pk, account_id__is_notable=True)
            .select_related("account_id")
        )

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

    @extend_schema(
        summary="Unified match event timeline",
        description=(
            "Returns a single, time-ordered timeline of item and ability events for the match.\n\n"
            "Optional query params:\n"
            "- `account_id`: only include events for one account\n"
            "- `notable_only`: if true, include only notable-player events\n"
        ),
        parameters=[
            OpenApiParameter(
                name="account_id",
                type=int,
                required=False,
                description="Only include events for one account_id.",
            ),
            OpenApiParameter(
                name="notable_only",
                type=bool,
                required=False,
                description="If true, include only events from notable players.",
                examples=[
                    OpenApiExample("Notable only", value=True),
                    OpenApiExample("Everyone", value=False),
                ],
            ),
        ],
    )
    @action(detail=True, methods=["get"])
    def events(self, request, pk=None):
        """
        GET /api/tracker/matches/<match_id>/events/
        """
        match_id = pk

        account_id = request.query_params.get("account_id")
        notable_only = (request.query_params.get("notable_only") or "").lower() in {
            "1", "true", "t", "yes", "y"
        }

        item_qs = (
            PlayerItem.objects
            .filter(match_id=match_id)
            .select_related("account_id", "item_id", "imbued_ability")
        )

        ability_qs = (
            PlayerAbility.objects
            .filter(match_id=match_id)
            .select_related("account_id", "ability_id")
        )

        if account_id:
            item_qs = item_qs.filter(account_id=account_id)
            ability_qs = ability_qs.filter(account_id=account_id)

        if notable_only:
            item_qs = item_qs.filter(account_id__is_notable=True)
            ability_qs = ability_qs.filter(account_id__is_notable=True)

        events = []

        for e in item_qs:
            events.append({
                "event_type": "shop_item",
                "game_time": e.game_time,
                "account_id": e.account_id.account_id,
                "username": e.account_id.username,
                "item_id": e.item_id.item_id,
                "item_name": e.item_id.name,
                "item_type": e.item_id.type,
                "cost": e.item_id.cost,
                "sold_time": e.sold_time,
                "is_upgrade": e.is_upgrade,
                "imbued_ability_id": e.imbued_ability.ability_id if e.imbued_ability else None,
                "imbued_ability_name": e.imbued_ability.name if e.imbued_ability else None,
            })

        for e in ability_qs:
            events.append({
                "event_type": "ability_upgrade",
                "game_time": e.game_time,
                "account_id": e.account_id.account_id,
                "username": e.account_id.username,
                "ability_id": e.ability_id.ability_id,
                "ability_name": e.ability_id.name,
                "hero_id": e.ability_id.hero_id,
            })

        events.sort(key=lambda x: (x["game_time"], x["event_type"]))

        return Response({
            "match_id": int(match_id),
            "count": len(events),
            "events": events,
        })


# ============================================================
# Heroes
# ============================================================
@extend_schema_view(
    list=extend_schema(summary="List heroes"),
    retrieve=extend_schema(summary="Retrieve a hero"),
)
class HeroViewSet(ReadOnlyModelViewSet):
    """
    GET /api/tracker/heroes/
    GET /api/tracker/heroes/<id>/
    """
    queryset = Hero.objects.all()
    serializer_class = HeroSerializer


# ============================================================
# Items
# ============================================================
@extend_schema_view(
    list=extend_schema(summary="List shop items"),
    retrieve=extend_schema(summary="Retrieve a shop item"),
)
class ItemViewSet(ReadOnlyModelViewSet):
    """
    GET /api/tracker/items/
    GET /api/tracker/items/<id>/
    """
    queryset = ShopItem.objects.all()
    serializer_class = ShopItemSerializer


# ============================================================
# Players
# ============================================================
@extend_schema_view(
    list=extend_schema(summary="List players"),
    retrieve=extend_schema(summary="Retrieve a player"),
)
class PlayerViewSet(ReadOnlyModelViewSet):
    """
    GET /api/tracker/players/
    GET /api/tracker/players/<id>/

    Extra actions:
    - GET /api/tracker/players/<account_id>/matches/
    """
    queryset = Account.objects.all()
    serializer_class = PlayerSerializer

    @extend_schema(
        summary="List matches for a player",
        description="Returns this player's performances across matches.",
    )
    @action(detail=True, methods=["get"])
    def matches(self, request, pk=None):
        """
        GET /api/tracker/players/<account_id>/matches/

        Returns this player's performances across matches.
        """
        performances = (
            PlayerPerformance.objects
            .filter(account_id=pk)
            .select_related("match_id", "match_id__avg_rank")
            .order_by("-match_id__date")
        )

        def format_duration(d):
            if d is None:
                return None
            total_seconds = int(d.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            if hours > 0:
                return f"{hours}:{minutes:02d}:{seconds:02d}"
            return f"{minutes}:{seconds:02d}"

        data = []
        for p in performances:
            match_dt = p.match_id.date
            match_dt_local = localtime(match_dt) if match_dt else None

            data.append({
                "match_id": p.match_id.match_id,
                "match_date": match_dt_local.strftime("%b %d, %Y %I:%M %p") if match_dt_local else None,
                "match_date_iso": match_dt.isoformat() if match_dt else None,
                "duration": format_duration(p.match_id.duration),
                "avg_rank_id": p.match_id.avg_rank_id,  # None if avg_rank is null
                "kills": p.kills,
                "deaths": p.deaths,
                "assists": p.assists,
                "is_win": p.is_win,
            })

        return Response(data)
