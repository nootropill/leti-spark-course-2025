from pyspark.shell import spark
from pyspark.sql import DataFrame

def read_csv(name: str) -> DataFrame:
    """
    Загрузка CSV-файла с именем {name}.csv из папки ../data относительно
    расположения скрипта (leti-spark-course-2024/lab1/solutions/).
    Возвращает Spark DataFrame с загруженными данными.
    """
    path = f"lab1/data/{name}.csv"
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

def view(view_name: str, query: str) -> None:
    """
    Создает или обновляет временное представление (view) с именем view_name
    по SQL-запросу query.
    """
    df = spark.sql(query)
    df.createOrReplaceTempView(view_name)

def solve() -> DataFrame:
    match = read_csv('match')
    player_result = read_csv('player_result')

    match.createOrReplaceTempView("match")
    player_result.createOrReplaceTempView("player_result")
    # создаём таблицу с результатом игрока: победа или поражение в конкретной игре
    view("ordered_results", """
        select
            pr.player_id,
            case when pr.is_radiant = m.radiant_won then 1 else 0 end as win_flag,
            m.finished_at as ts,
            pr.match_id,
            pr.is_radiant
        from player_result pr
        join match m on pr.match_id = m.match_id
        order by player_id, ts
    """)

    # считаем группы проигрышей для каждого игрока
    view("with_loss_groups", """
        select
            player_id,
            win_flag,
            sum(case when win_flag = 0 then 1 else 0 end)
                over (partition by player_id order by ts rows between unbounded preceding and current row) as loss_group
        from ordered_results
    """)

    # длина каждой серии побед
    view("streaks", """
        select
            player_id,
            loss_group,
            sum(win_flag) over (partition by player_id, loss_group) as streak_len
        from with_loss_groups
    """)

    # максимальная серия побед для каждого игрока
    view("max_win_streak", """
        select player_id, max(streak_len) as max_win_streak
        from streaks
        group by player_id
    """)
    all_max_win_streak = spark.sql("""
        select player_id, max_win_streak
        from max_win_streak
        order by max_win_streak desc
    """)
    all_max_win_streak.show(59, truncate=False)
    # формируем все пары игроков, которые были в одной команде в одном матче
    view("player_pairs", """
        select
            pr1.player_id as player1,
            pr2.player_id as player2,
            pr1.match_id,
            pr1.is_radiant,
            case when pr1.is_radiant = m.radiant_won then 1 else 0 end as win_flag,
            m.finished_at as ts
        from player_result pr1
        join player_result pr2
            on pr1.match_id = pr2.match_id
            and pr1.is_radiant = pr2.is_radiant
            and pr1.player_id < pr2.player_id
        join match m on m.match_id = pr1.match_id
        order by player1, player2, ts
    """)

    # группы проигрышей для каждой пары
    view("pair_with_loss_groups", """
        select
            player1, player2, win_flag,
            sum(case when win_flag = 0 then 1 else 0 end)
                over (partition by player1, player2 order by ts rows between unbounded preceding and current row) as loss_group
        from player_pairs
    """)

    # длина серии побед для каждой пары
    view("pair_streaks", """
        select
            player1, player2, loss_group,
            sum(win_flag) over (partition by player1, player2, loss_group) as streak_len
        from pair_with_loss_groups
    """)

    # максимальная серия побед для каждой пары
    view("max_pair_win_streak", """
        select player1, player2, max(streak_len) as pair_win_streak
        from pair_streaks
        group by player1, player2
    """)

    # join max_win_streak для player1 и player2 отдельно, чтобы избежать дублирования
    view("pair_max_win_streak_fixed", """
        select
            mpps.player1,
            mpps.player2,
            mpps.pair_win_streak,
            least(mx1.max_win_streak, mx2.max_win_streak) as min_max_win_streak
        from max_pair_win_streak mpps
        join max_win_streak mx1 on mpps.player1 = mx1.player_id
        join max_win_streak mx2 on mpps.player2 = mx2.player_id
    """)

    # разница между минимальной индивидуальной серией и парной серией
    view("pair_win_streak_diff_fixed", """
        select
            player1,
            player2,
            pair_win_streak,
            min_max_win_streak,
            min_max_win_streak - pair_win_streak as win_streak_diff
        from pair_max_win_streak_fixed
    """)

    # финальный результат для анализа
    df = spark.sql("""
        select * from pair_win_streak_diff_fixed
        order by win_streak_diff desc nulls last
    """)
    df.show(1000, truncate=False)

    return df

if __name__ == "__main__":
    df = solve()
