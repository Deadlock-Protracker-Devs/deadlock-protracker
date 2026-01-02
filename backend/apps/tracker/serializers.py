from rest_framework import serializers
from .models import Hero, ShopItem, Match, Account, PlayerPerformance

class HeroSerializer(serializers.ModelSerializer):
    class Meta:
        model = Hero
        fields = ["hero_id", "name", "icon_key"]

class ShopItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShopItem
        fields = [
            "item_id",
            "name",
            "icon_key",
            "type",
            "cost",
            "imbue",
            "upgrades_into",
        ]
        
class PlayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Account
        fields = ["account_id", "username"]

class MatchSerializer(serializers.ModelSerializer):
    avg_rank = serializers.StringRelatedField()

    class Meta:
        model = Match
        fields = [
            "match_id",
            "date",
            "duration",
            "avg_rank",
        ]

