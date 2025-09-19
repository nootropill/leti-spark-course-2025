import re

# Импорты из PySpark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StringType, ArrayType

# Правильный относительный импорт, чтобы найти common.py в родительской директории
from ..common import SparkContextCommon, run_solution


# --- Вспомогательные функции ---

def clean_data(input_code: str) -> str:
    """
    Полностью очищает и нормализует JavaScript код, приводя его к единой строке
    без комментариев, форматирования и с унифицированными именами.
    """
    # Удаляем однострочные и многострочные комментарии
    input_code = re.sub(r"(\/\*[\s\S]*?\*\/)|(\/\/.*?$)", "", input_code, flags=re.MULTILINE)
    # Заменяем модификаторы доступа и this
    input_code = re.sub(r"\b(public|private|protected)\b", "const", input_code)
    input_code = re.sub(r"\bthis\.", "", input_code)
    # Обработка стрелочных функций
    arrow_functions = r"(const|let)\s+([a-zA-Z_$]\w*)\s*(?::\s*\w+\s*)?=\s*(?:\(?)([^)\n]*)(?:\)?)\s*=>\s*(?:<.*?>)?\s*(\{?[^}]*\}?|.+?)"
    input_code = re.sub(arrow_functions, r"function \2(\3) \4", input_code)
    # Сбор объявлений функций и параметров для стандартизации
    function_decls = re.finditer(
        r"\bfunction\s+([a-zA-Z_$][\w$]*)\s*\(([^)]*)\)",
        input_code
    )
    counter = 0
    var_map = {}
    for match in function_decls:
        func_name, params = match.groups()
        if func_name not in var_map:
            var_map[func_name] = f"func{counter}"
            counter += 1
        for param in re.split(r"\s*,\s*", params):
            if param and param not in var_map:
                var_map[param] = f"var{counter}"
                counter += 1
    # Стандартизация объявлений переменных
    var_counter = [0]
    declarations = re.finditer(
        r"\b(var|let|const|function|class)\s+([a-zA-Z_$][\w$]*)\b",
        input_code
    )
    for match in declarations:
        var_type, var_name = match.groups()
        if var_name not in var_map:
            var_counter[0] += 1
            var_map[var_name] = f"{var_type}{var_counter[0]}"
    def replace_var(match: str):
        word = match.group(0)
        return var_map.get(word, word)
    # Заменяем все идентификаторы на стандартизированные
    input_code = re.sub(
        r"\b[a-zA-Z_$][\w$]*\b",
        replace_var,
        input_code
    )
    # Стандартизация кавычек, удаление import/export и аннотаций типов
    input_code = re.sub(r"['\"`]", "'", input_code)
    input_code = re.sub(r"^\s*(import|export).*?;\s*$", "", input_code, flags=re.MULTILINE)
    input_code = re.sub(r":\s*\w+", "", input_code)
    # Удаляем лишние пробелы и выравниваем переносы строк
    input_code = re.sub(r"(\s)+", " ", input_code)
    input_code = re.sub(r"\s*[\r\n]+\s*", "\n", input_code)
    # Финальная нормализация — удаляем все пробелы, превращая код в одну строку
    return "".join(input_code.split())


def create_n_gramms(input_str: str) -> list:
    """
    Разбивает входную строку на n-граммы (подстроки) фиксированной длины.
    """
    substr_size = 5
    result = []
    # Проходим по строке "скользящим окном"
    for i in range(len(input_str) - substr_size + 1):
        result.append(input_str[i: i + substr_size])
    return result


# --- Регистрация UDF для использования в Spark ---

clean_data_udf = udf(lambda code: clean_data(code), StringType())
n_gramms_udf = udf(lambda text: create_n_gramms(text), ArrayType(StringType()))


# --- Основная функция решения ---

def solve(common: SparkContextCommon) -> DataFrame:
    """
    Основная функция, выполняющая все этапы анализа плагиата.
    """
    # Этап 1: Чтение данных с помощью стандартной функции из common.py
    df = common.read_data()

    # Защита от случая, когда нет входных файлов
    if df.rdd.isEmpty():
        return common.spark.createDataFrame([], "target string, source string, plagiarism_coefficient double")

    # Этап 2: Нормализация кода и создание n-грамм
    df = df.withColumn("content", n_gramms_udf(clean_data_udf(col("content"))))
    df.createOrReplaceTempView("data")

    # Этап 3: "Взрываем" массив n-грамм в отдельные строки
    common.view("exploded_contents", """
        SELECT
            author AS file_src,
            explode(content) AS exploded_content
        FROM data
    """)

    # Этап 4: Хешируем каждую n-грамму с помощью MD5
    common.view("hashed_n_gramms", """
        SELECT
            file_src,
            md5(exploded_content) AS hash
        FROM exploded_contents
    """)

    # Этап 5: Считаем количество уникальных хешей для каждого файла
    common.view("file_hash_counts", """
        SELECT
            file_src,
            count(DISTINCT hash) AS hash_count
        FROM hashed_n_gramms
        GROUP BY file_src
    """)

    # Этап 6: Вычисляем коэффициент плагиата
    common.view("result", """
        SELECT
            target.file_src AS target,
            source.file_src AS source,
            -- Формула: (кол-во общих уник. хешей) / (мин. кол-во уник. хешей в одном из файлов)
            ROUND(COUNT(DISTINCT target.hash) / MIN(fhs.hash_count), 2) AS plagiarism_coefficient
        FROM hashed_n_gramms AS target
        JOIN hashed_n_gramms AS source
            ON target.hash = source.hash AND target.file_src != source.file_src
        JOIN file_hash_counts AS fhs
            ON target.file_src = fhs.file_src
        GROUP BY target.file_src, source.file_src
        ORDER BY plagiarism_coefficient DESC, target ASC, source ASC
    """)

    # Получаем итоговый DataFrame
    df_result = common.spark.sql("SELECT * FROM result")

    # Выводим полную таблицу в консоль без обрезания строк
    print("--- Итоговая таблица коэффициентов плагиата ---")
    df_result.show(n=df_result.count(), truncate=False)

    return df_result


if __name__ == "__main__":
    # Получаем имя текущего файла без расширения .py
    import os
    module_name = os.path.splitext(os.path.basename(__file__))[0]
    
    # Вызываем run_solution с именем модуля, как она и ожидает
    run_solution(module_name)
