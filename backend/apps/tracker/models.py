from django.db import models

# static tables

class Hero(models.Model):
    # static hero data
    def __str__(self):
        return self.name

    hero_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=50)
    icon_key = models.CharField(max_length=30)

class Ability(models.Model):
    # static ability data, each belonging to a hero
    def __str__(self):
        return self.name

    ability_id = models.PositiveBigIntegerField(primary_key=True)
    name = models.CharField(max_length=50)
    icon_key = models.CharField(max_length=50)
    hero = models.ForeignKey(Hero, on_delete=models.CASCADE)

class ShopItem(models.Model):
    # static item data
    def __str__(self):
        return self.name


    class ItemType(models.TextChoices):
        SPIRIT = "spirit"
        WEAPON = "weapon"
        VITALITY = "vitality"

    item_id = models.PositiveBigIntegerField(primary_key=True)
    name = models.CharField(max_length=50)
    icon_key = models.CharField(max_length=50)

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
    # static account data, which will be maintained manually and is comprised of pro players
    def __str__(self):
        return self.username

    account_id = models.BigIntegerField(primary_key=True)
    username = models.CharField(max_length=50)

class Rank(models.Model):
    # static rank data 
    def __str__(self):
        return self.name

    rank_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=30)
    icon_key = models.CharField(max_length=30)

# dynamic tables

class Match(models.Model):
    def __str__(self):
        return str(self.match_id)
    
    match_id = models.BigIntegerField(primary_key=True)
    date = models.DateTimeField()
    duration = models.DurationField()
    avg_rank = models.ForeignKey(
        Rank, 
        on_delete=models.PROTECT,
        null=True,
        blank=True,
    )

class PlayerPerformance(models.Model):
    # composite primary key is not available in this version, will just have to use django assigned auto primary key 
    # pk = models.CompositePrimaryKey("match_id", "account_id")
    def __str__(self):
        if self.account_id and self.account_id.username:
            account_display = f"account: {self.account_id.username}"
        else:
            account_display = f"account: {self.account_id_id}"

        if self.is_win:
            result = "win"
        else:
            result = "loss"

        return f"{account_display} | match: {self.match_id_id} | {result}"

    account_id = models.ForeignKey("Account", on_delete=models.CASCADE)
    match_id = models.ForeignKey("Match", on_delete=models.CASCADE)

    kills = models.SmallIntegerField()
    deaths = models.SmallIntegerField()
    assists = models.SmallIntegerField()

    networth = models.IntegerField()

    team = models.SmallIntegerField()
    is_win = models.BooleanField()
    
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["account_id", "match_id"], name="uniq_perf_account_match")
        ]

class PlayerAbility(models.Model):
    account_id = models.ForeignKey("Account", on_delete=models.CASCADE) 
    match_id = models.ForeignKey("Match", on_delete=models.CASCADE)
    ability_id = models.ForeignKey("Ability", on_delete=models.CASCADE)

    game_time = models.IntegerField() # level time

class PlayerItem(models.Model):
    account_id = models.ForeignKey("Account", on_delete=models.CASCADE) 
    match_id = models.ForeignKey("Match", on_delete=models.CASCADE)
    item_id = models.ForeignKey("ShopItem", on_delete=models.CASCADE)

    game_time = models.IntegerField() # purchase time
    sold_time = models.IntegerField() # sell time

    is_upgrade = models.BooleanField(null=True)
    imbued_ability = models.ForeignKey("Ability", on_delete=models.CASCADE)
