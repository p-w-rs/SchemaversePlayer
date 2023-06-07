import psycopg
import collections
import time
import random


Player = collections.namedtuple("Player", "id username balance fuel_reserve")
Planet = collections.namedtuple(
    "Planet", "id name mine_limit location_x location_y conqueror_id location"
)
Ship = collections.namedtuple(
    "Ship",
    "id fleet_id player_id name last_action_tic last_move_tic last_living_tic current_health max_health current_fuel max_fuel max_speed range attack defense engineering prospecting location_x location_y direction speed destination_x destination_y repair_priority action action_target_id location destination target_speed target_direction",  # noqa: E501
)
Fleet = collections.namedtuple(
    "Fleet", "id name script script_declarations last_script_update_tic enabled runtime"
)
ShipType = collections.namedtuple(
    "ShipType", "name attack defense engineering prospecting"
)


def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func

    return decorate


def get_my_player(cur):
    cur.execute("SELECT id, username, balance, fuel_reserve FROM my_player;")
    return cur.fetchone()


def get_my_planets(cur):
    cur.execute("SELECT * FROM planets WHERE conqueror_id=GET_PLAYER_ID(SESSION_USER);")
    return cur.fetchall()


def get_my_ships(cur):
    cur.execute("SELECT * FROM my_ships;")
    return cur.fetchall()


def get_my_fleets(cur):
    cur.execute("SELECT * FROM my_fleets;")
    return cur.fetchall()


def get_ships_on(cur, planet):
    cur.execute(
        """
        SELECT * FROM planets WHERE name=%s
        INNER JOIN my_ships ON my_ships.location=planets.location
        RETURNING *;
        """,
        (planet.name),
    )
    return cur.fetchall()


def can_add_miner(cur, planet):
    ships = get_ships_on(cur, planet)
    n = 0
    for ship in ships:
        if ship.prospecting >= 20:
            n += 1
    if n >= planet.mine_limit:
        return False
    return True


def can_add_sniper_defender(cur, planet):
    ships = get_ships_on(cur, planet)
    n = 0
    for ship in ships:
        if ship.attack >= 15 and ship.defense >= 5:
            n += 1
    if n >= 2:
        return False
    return True


def can_add_engineer_defender(cur, planet):
    ships = get_ships_on(cur, planet)
    n = 0
    for ship in ships:
        if ship.engineering >= 15 and ship.defense >= 5:
            n += 1
    if n >= 1:
        return False
    return True


@static_vars(n=0)
def create_ship(cur, planet, ship_type):
    cur.execute(
        """
            INSERT INTO my_ships(name, attack, defense, engineering, prospecting, location)
            VALUES(%s,%s,%s,%s,%s,POINT(%s,%s)) RETURNING *;
            """,  # noqa: E501
        (
            "{}_{}_{}".format(ship_type.name, planet.name, create_ship.n),
            ship_type.attack,
            ship_type.defense,
            ship_type.engineering,
            ship_type.prospecting,
            planet.location_x,
            planet.location_y,
        ),
    )
    create_ship.n += 1
    return cur.fetchone()


def mine_cont(cur, planet, ship):
    cur.execute(
        "UPDATE my_ships SET action='MINE', action_target_id=%s WHERE id=%s",
        (planet.id, ship.id),
    )
    return cur.fetchone()


def refuel_ships(cur):
    cur.execute("SELECT REFUEL_SHIP(id) FROM my_ships WHERE current_fuel < max_fuel;")
    return cur.fetchone()


def get_money(cur):
    player = Player(*get_my_player(cur))
    cur.execute(
        "SELECT CONVERT_RESOURCE('FUEL', %s) as Converted FROM my_player;",
        (player.fuel_reserve // 2),
    )
    return cur.fetchone()


def repair_ships(cur):
    used_ids = {}
    cur.execute("SELECT * FROM my_ships WHERE current_health < max_health")
    inj_ships = list(map(lambda x: Ship(*x), cur.fetchall()))
    for inj_ship in inj_ships:
        cur.execute(
            "SELECT * FROM my_ships WHERE engineering >= 20, location=%s",
            (inj_ship.location),
        )
        eng_ships = list(map(lambda x: Ship(*x), cur.fetchall()))
        for eng_ship in eng_ships:
            if eng_ship.id not in used_ids:
                cur.execute(
                    "SELECT REPAIR(%s, %s);",
                    (eng_ship.id, inj_ship.id),
                )
                cur.fetchone()
                used_ids.add(eng_ship.id)
                break


def build_mining_ships(cur):
    player = Player(*get_my_player(cur))
    planets = list(map(lambda x: Planet(*x), get_my_planets(cur)))
    ship_type = ShipType("prospecting", 0, 0, 0, 20)
    for planet in planets:
        if can_add_miner(planet) and player.balance >= 1000:
            ship = create_ship(cur, planet, ship_type)
            mine_cont(cur, planet, ship)


def build_defense_ships(cur):
    player = Player(*get_my_player(cur))
    planets = list(map(lambda x: Planet(*x), get_my_planets(cur)))
    ship_type_1 = ShipType("snp_def", 15, 5, 0, 0)
    ship_type_2 = ShipType("snp_def", 0, 5, 15, 0)
    for planet in planets:
        if can_add_sniper_defender(planet) and player.balance >= 1000:
            create_ship(cur, planet, ship_type_1)
        if can_add_engineer_defender(planet) and player.balance >= 1000:
            create_ship(cur, planet, ship_type_2)


def set_fleet_id(cur, ships, fleet_id):
    for ship in ships:
        cur.execute("UPDATE my_ships SET fleet_id=%s WHERE id=%s;", (fleet_id, ship.id))
        cur.fetchone()
    return cur.fetchone()


def build_attack_fleet(cur):
    player = Player(*get_my_player(cur))
    if player.balance < 9000:
        return 0
    planets = list(map(lambda x: Planet(*x), get_my_planets(cur)))
    ship_snp = ShipType("snp", 20, 0, 0, 0)
    ship_btl = ShipType("btl", 10, 10, 0, 0)
    ship_eng = ShipType("eng", 0, 0, 20, 0)
    ship_pro = ShipType("travel_pro", 0, 19, 0, 1)
    cur.execute("SELECT MAX(fleet_id) FROM my_ships;")
    fleet_id = cur.fetchone()
    if fleet_id:
        cur.execute("SELECT COUNT(*) FROM my_ships WHERE fleet_id=%s;", (fleet_id))
        n = cur.fetchone()
        if n >= 8:
            fleet_id += 1
    else:
        fleet_id = 1

    planet = random.choice(planets)
    s1 = create_ship(cur, planet, random.choice([ship_snp, ship_btl]))
    s2 = create_ship(cur, planet, random.choice([ship_snp, ship_btl]))
    s3 = create_ship(cur, planet, random.choice([ship_snp, ship_btl]))
    s4 = create_ship(cur, planet, random.choice([ship_snp, ship_btl]))
    s5 = create_ship(cur, planet, random.choice([ship_snp, ship_btl]))
    s6 = create_ship(cur, planet, random.choice([ship_snp, ship_btl]))
    s7 = create_ship(cur, planet, ship_eng)
    s8 = create_ship(cur, planet, ship_eng)
    s9 = create_ship(cur, planet, ship_pro)
    set_fleet_id(cur, [s1, s2, s3, s4, s5, s6, s7, s8, s9], fleet_id)


def upgrade_ships(cur):
    cur.execute("SELECT MAX(fleet_id) FROM my_ships;")
    fleet_id = cur.fetchone()
    for i in range(1, fleet_id + 1):
        player = Player(*get_my_player(cur))
        if player.balance // 2 >= 1075:
            cur.execute("SELECT * FROM my_ships WHERE fleet_id=%s;", (i))
            ships = list(map(lambda x: Ship(*x), cur.fetchall()))
            for ship in ships:
                cur.execute(
                    """SELECT id, 
                           UPGRADE(id, 'MAX_FUEL', 25), 
                           UPGRADE(id, 'MAX_SPEED', 25), 
                           UPGRADE(id, 'RANGE', 1)
                         FROM my_ships 
                         WHERE id=%s;
                    """,
                    (ship.id),
                )
                cur.fetchone()
                if ship.attack > ship.defense:
                    cur.execute(
                        "SELECT id, UPGRADE(id, 'ATTACK', 2) FROM my_ships WHERE id=%s;",  # noqa: E501
                        (ship.id),
                    )
                    cur.fetchone()
                elif ship.engineering > 0:
                    cur.execute(
                        "SELECT id, UPGRADE(id, 'ENGINEERING', 2) FROM my_ships WHERE id=%s;",  # noqa: E501
                        (ship.id),
                    )
                    cur.fetchone()
                elif ship.attack == ship.defense:
                    cur.execute(
                        "SELECT id, UPGRADE(id, 'ATTACK', 1), UPGRADE(id, 'DEFENSE', 1) FROM my_ships WHERE id=%s;",  # noqa: E501
                        (ship.id),
                    )
                    cur.fetchone()
        else:
            return 0


def get_close_planet(cur, location):
    cur.execute()


def set_dsts_attack(cur):
    cur.execute("SELECT MAX(fleet_id) FROM my_ships;")
    fleet_id = cur.fetchone()
    for i in range(1, fleet_id + 1):
        cur.execute("SELECT * FROM my_ships WHERE fleet_id=%s;", (i))
        ships = list(map(lambda x: Ship(*x), cur.fetchall()))
        planet = get_close_planet(cur, ships[0].location)
        cur.execute(
            """
            SELECT * SHIP_COURSE_CONTROL(id, current_fuel / 2 , null , POINT(1337, 1337)) 
            FROM my_ships WHERE fleet_id=%s;
            """,
            (i),
        )
        
        for ship in ships:
            


def play_tic(cur):
    # player = Player(*get_my_player(cur))
    # planets = list(map(lambda x: Planet(*x), get_my_planets(cur)))
    # ships = list(map(lambda x: Ship(*x), get_my_ships(cur)))
    # fleets = list(map(lambda x: Fleet(*x), get_my_fleets(cur)))

    refuel_ships(cur)
    get_money(cur)
    repair_ships(cur)

    build_mining_ships(cur)
    build_defense_ships(cur)
    build_attack_fleet(cur)

    upgrade_ships(cur)
    set_attack_dsts(cur)


def get_tic(cur):
    cur.execute("SELECT last_value FROM tic_seq;")
    return cur.fetchone()[0]


def main():
    LAST_TIC = 0
    CUR_TIC = 0

    while True:
        with psycopg.connect(
            "host=db.schemaverse.com dbname=schemaverse user=jpp46 password=wwpfdfad"
        ) as conn:
            with conn.cursor() as cur:
                CUR_TIC = get_tic(cur)
                if CUR_TIC > LAST_TIC:
                    play_tic(cur)
                conn.commit()
                LAST_TIC = CUR_TIC
        time.sleep(15)


if __name__ == "__main__":
    main()
