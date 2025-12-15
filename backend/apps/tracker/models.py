from django.db import models

## static tables

class Hero(models.Model):
    hero_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=50)
    icon_key = models.CharField(max_length=30)

class Ability(models.Model):
    ability_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=30)
    icon_key = models.CharField(max_length=30)
    hero = models.ForeignKey(Hero, on_delete=models.CASCADE)

class ShopItem(models.Model):

    class ItemType(models.TextChoices):
        SPIRIT = "Spirit"
        GUN = "Gun"
        VITALITY = "Vitality"

    item_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=30)
    icon_key = models.CharField(max_length=30)

    imbue = models.BooleanField()
    type = models.CharField(
        max_length=20,
        choices=ItemType.choices,
    )
    cost = models.IntegerField()
    upgrades_into = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="upgraded_from"
    )
    
class Account(models.Model):
    account_id = models.IntegerField(primary_key=True)
    username = models.CharField(max_length=30)

class Rank(models.Model):
    rank_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=30)
    icon_key = models.CharField(max_length=30)

## dynamic tables

class Match(models.Model):
    match_id = models.IntegerField(primary_key=True)
    date = models.DateTimeField(auto_now_add=True)
    duration = models.DurationField()
    avg_rank = models.ForeignKey(Rank, on_delete=models.PROTECT)

class PlayerPerformance(models.Model):
    # composite primary key is not available in this version, will just have to use django assigned auto primary key 
    # pk = models.CompositePrimaryKey("match_id", "account_id")

    account_id = models.ForeignKey("Account", on_delete=models.CASCADE)
    match_id = models.ForeignKey("Match", on_delete=models.CASCADE)

    kills = models.PositiveSmallIntegerField()
    deaths = models.PositiveSmallIntegerField()
    assists = models.PositiveSmallIntegerField()

    networth = models.IntegerField()

    team = models.SmallIntegerField()
    is_win = models.BooleanField()

class PlayerAbility(models.Model):
    account_id = models.ForeignKey("Account", on_delete=models.CASCADE) 
    match_id = models.ForeignKey("Match", on_delete=models.CASCADE)
    ability_id = models.ForeignKey("Ability", on_delete=models.CASCADE)

    game_time = models.PositiveIntegerField() ## purchase time

class PlayerItem(models.Model):
    account_id = models.ForeignKey("Account", on_delete=models.CASCADE) 
    match_id = models.ForeignKey("Match", on_delete=models.CASCADE)
    item_id = models.ForeignKey("ShopItem", on_delete=models.CASCADE)

    game_time = models.PositiveIntegerField() ## purchase time
    sold_time = models.PositiveIntegerField() ## purchase time

    is_upgrade = models.BooleanField()
    imbued_ability = models.ForeignKey("Ability", on_delete=models.CASCADE)
