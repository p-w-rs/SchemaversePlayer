import psycopg
import collections
import time


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
    if len(get_ships_on(cur, planet)) >= planet.mine_limit:
        return False
    return True


@static_vars(n=0)
def create_mining_ship(cur, planet):
    cur.execute(
        """
            INSERT INTO my_ships(name, attack, defense, engineering, prospecting, location)
            VALUES(ms_%s,%s,%s,%s,%s,POINT(%s,%s)) RETURNING *;
            """,  # noqa: E501
        (create_mining_ship.n, 0, 0, 0, 20, planet.location_x, planet.location_y),
    )
    create_mining_ship.n += 1
    return cur.fetchone()


@static_vars(n=0)
def create_eng_ship(cur, planet):
    cur.execute(
        """
            INSERT INTO my_ships(name, attack, defense, engineering, prospecting, location)
            VALUES(es_%s,%s,%s,%s,%s,POINT(%s,%s)) RETURNING *;
            """,  # noqa: E501
        (create_eng_ship.n, 0, 5, 15, 0, planet.location_x, planet.location_y),
    )
    create_eng_ship.n += 1
    return cur.fetchone()


@static_vars(n=0)
def create_snp_ship(cur, planet):
    cur.execute(
        """
            INSERT INTO my_ships(name, attack, defense, engineering, prospecting, location)
            VALUES(ss_%s,%s,%s,%s,%s,POINT(%s,%s)) RETURNING *;
            """,  # noqa: E501
        (create_snp_ship.n, 20, 0, 0, 0, planet.location_x, planet.location_y),
    )
    create_snp_ship.n += 1
    return cur.fetchone()


@static_vars(n=0)
def create_btt_ship(cur, planet):
    cur.execute(
        """
            INSERT INTO my_ships(name, attack, defense, engineering, prospecting, location)
            VALUES(bs_%s,%s,%s,%s,%s,POINT(%s,%s)) RETURNING *;
            """,  # noqa: E501
        (create_btt_ship.n, 10, 10, 0, 0, planet.location_x, planet.location_y),
    )
    create_btt_ship.n += 1
    return cur.fetchone()


def mine_cont(cur, planet, ship):
    cur.execute(
        "UPDATE my_ships SET action='MINE', action_target_id=%s WHERE id=%s",
        (planet.id, ship.id),
    )


def play_tic(cur):
    player = Player(*get_my_player(cur))
    planets = list(map(lambda x: Planet(*x), get_my_planets(cur)))
    ships = list(map(lambda x: Ship(*x), get_my_ships(cur)))
    # fleets = list(map(lambda x: Fleet(*x), get_my_fleets(cur)))

    # We are just starting the round or have been reduced to similar settings
    if len(planets) == 1 and not ships and player.balance >= 1000:
        for _ in range(0, min(player.balance // 1000, planets[0].mine_limit)):
            ship = Ship(*create_mining_ship(cur, planets[0]))
            mine_cont(cur, planets[0], ship)
    else:
        x = 1


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
