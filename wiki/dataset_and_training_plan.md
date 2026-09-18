# План: сборка датасета и запуск обучения (AutoGluon TimeSeries)

> Цель: собрать датасет «магазин × товар × день → продажи» с признаками
> (промо, цена, иерархии товара и магазина) и обучить прогноз.
> Два пути: **(A)** готовый пайплайн `ml_ag` и **(B)** ручная сборка в pandas.

---

## 0. Общая схема потока данных (профиль `r` в ml_ag)

```
ClickHouse (db=remi)                      clickhouse-local (SQL)
      │  src/sql/09.sql (Jinja2)                 │  src/sql/prepare_data/main.sql
      ▼                                          ▼
sales_dataset.parquet ──────────► prepared_ts_dataframe.parquet
(широкие агрегаты по дням)        + prepared_ts_dataframe.static.parquet
      │                                          │
      ▼                                          ▼
                                train_model (AutoGluon TimeSeriesPredictor)
                                → autogluon_model/  leaderboard.yaml  feature_importance.yaml
                                predict_model → raw_predictions_ts.parquet
                                evaluate_model → *_metrics.yaml
```

Артефакты и функции:

| Задача | Функция | Вход → Выход |
|---|---|---|
| `build_sql_dataset` | `tasks.data_preparation.create_dataset_from_ch` | CH → `sales_dataset.parquet` |
| `prepare_data_sql` | `tasks.data_prep.preparation_sql.prepare_timeseries_data_sql` | parquet → `prepared_ts_dataframe(.static).parquet` |
| `train_model` | `tasks.training.training.train_model` | parquet → модель + leaderboard |
| `predict_model` | `tasks.prediction.prediction.predict_model` | модель → прогнозы |
| `evaluate_model` | `tasks.evaluation.evaluation.evaluate_model` | прогнозы → метрики |

Сериализация TS-датасета: `tasks/io/timeseries.py` — `TimeSeriesDataFrame` сохраняется
как `X.parquet` + `X.static.parquet` (static features отдельным файлом).

---

## 1. Формат данных (что ждёт TimeSeriesPredictor)

### 1.1 Основная таблица — long format

`item_id` = каждая пара (магазин, товар) = отдельный ряд.
Колонки: `item_id`, `timestamp` (datetime), `target`, затем covariates.

| Колонка | Роль |
|---|---|
| `item_id` | `"store_1_product_2"` (составной из LocationId + ItemId) |
| `timestamp` | `datetime` |
| `target` | объём продаж (float) |
| `promo_flag`, `promo_type` | **known covariates** (известны на горизонт заранее) |
| `price`, `PriceChangePct`, `PriceVsRollingMedian28d` | **past covariates** (будущее неизвестно) |

```python
from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor

df["item_id"] = df["LocationId"].astype(str) + "_" + df["ItemId"].astype(str)
tsdf = TimeSeriesDataFrame.from_data_frame(
    df,
    id_column="item_id",
    timestamp_column="date",
)
```

Требования:
- минимум `max(prediction_length + 1, 5) + prediction_length` точек на часть рядов;
- пропуски дат: указать `freq="D"` — AutoGluon достроит сетку; нулевые продажи
  лучше явно 0, а не NaN;
- `static_features` должен покрывать все `item_id` (иначе исключение).

### 1.2 Static features — отдельный DataFrame

Одна строка на `item_id`. **Критично:** категории — dtype `category`/`object`,
иначе AutoGluon посчитает их непрерывными.

```python
static = catalog.merge(store_info, on="store")
static["item_id"] = static["LocationId"].astype(str) + "_" + static["ItemId"].astype(str)
cat_cols = ["group", "subgroup", "brand", "network", "format", "region"]
static[cat_cols] = static[cat_cols].astype("category")
tsdf.static_features = static.set_index("item_id")
```

> В пайплайне ml_ag то же самое делает `use_id_columns_as_static_features: true`
> (`preparation.py:89-101`): id-колонки приводятся к `str` ДО попадания в static.

### 1.3 Иерархии товара и магазина

- Передавать каждый уровень **отдельной категориальной колонкой** (группа,
  подгруппа, под-подгруппа; сеть, формат, регион). НЕ схлопывать в один признак.
- AutoGluon сам выберет кодирование под модель: деревья (LightGBM/CatBoost) —
  сплиты по подмножествам категорий, нейросети (DeepAR/TFT) — embeddings.
- Свое кодирование не делать: **порядковое (1,2,3) ломает смысл**, one-hot
  хуже embedding для NN. Максимум — `.astype("category")`.
- Высокая кардинальность (`store_id` на тысячи магазинов): лучше заменить на
  признаки магазина (сеть, формат, регион, площадь) — точнее и меньше колонок.
- AutoGluon **не умеет реконсиляцию уровней** (иерархический прогноз) —
  если нужна согласованность сумм по уровням, делать вручную (bottom-up).

### 1.4 Цена и инфляция

Проблема «цена 100 год назад ≈ 110 сейчас» решается нормировкой. В пайплайне
уже есть (main.sql):
- `Price` — ffill фактической цены (`SalesAmount/SalesQty`) по ряду;
- `PriceChangePct` = `(Price - prev_price)/prev_price`;
- `PriceVsRollingMedian28d` = `Price / rolling_median_28d - 1` — **относительная
  цена, инфляция снимается**;
- `IsPromo` = скидка ≥5% от предыдущей цены (не исходный флаг).

Если будущая цена известна — сделать её known covariate
(`use_current_price_for_future_covariates: true`, подставляется `CurrentPrice`
из `InventItemLocation`). Иначе оставить past covariate (AutoGluon на горизонте
продлит последнее значение ffill).

### 1.5 Промо

`promo_flag` (0/1) + `promo_type` (категориальная) + при желании десятки
производных (в пайплайне: `DaysSincePromoStart`, `IsPostPromo`, `PromoLiftRatio`
и т.д., ~40 шт.). Если план промо известен на горизонт — **known covariates**
(обязательно указать в `known_covariates_names`).

---

## 2. Вариант A: готовый пайплайн ml_ag

### 2.1 Конфиг — profile.yaml

База — `data/04/r/profile.yaml`. Ключевые секции:

```yaml
tasks:
  build_sql_dataset:
    function: tasks.data_preparation.create_dataset_from_ch
    params:
      db_name: remi
      execution_mode: client      # через clickhouse-client
      delivery_mode: disk         # сразу пишем parquet
      sql_template_path: sql/09.sql
      render_params:
        MaxItemGap: 1
        MaxStoreGap: 3
        RecencyDays: 7
        ValidationDays: 30
        Sum: [SalesAmount, SalesCostAmount, SalesProfitAmount, StockQty, ...]
        # включает PriceRaw = SalesAmount/SalesQty, промо-источники и т.д.

  prepare_data_sql:
    function: tasks.data_prep.preparation_sql.prepare_timeseries_data_sql
    params:
      input_artifact_name: sales_dataset
      output_artifact_name: prepared_ts_dataframe
      sql_template_path: sql/prepare_data/main.sql
      id_columns: [LocationId, ItemId]      # + ItemMainEd при granularity
      use_id_columns_as_static_features: true
      static_features_to_gen: [MeanSales_Item, MeanSales_Location]
      prediction_length: 1
      preprocessing:
        clip_negatives_to_zero: true
        trim_outliers_by_quantile: true
        outlier_quantile_value: 0.999

  train_model:
    function: tasks.training.training.train_model
    params:
      predictor_init_args:
        freq: D
        eval_metric: WAPE
        quantile_levels: [0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        verbosity: 2
      fit_args:
        excluded_model_types: [Chronos2, Chronos2SmallFineTuned, AutoETS, DynamicOptimizedTheta]
      calc_fi: true
      prediction_length: 1
```

### 2.2 Запуск

Локально (из `ml_ag/`):

```sh
uv run -m src.main r -c src/conf/demo.yaml -s settings.prefect_enabled=false
```

Переопределение параметров через `-s key.path=value`:

```sh
# сменить горизонт, включить промо-признаки
uv run -m src.main r -c src/conf/demo.yaml \
  -s tasks.train_model.params.fit_args.hyperparameters.DeepAR={}
```

Продакшен-обвязка (docker, см. `readme.md`):

```sh
src/bin/ml_pipe.sh r -p -af          # полный пайплайн профиля r
src/bin/ml_run.sh rd -d              # promo + ds_r + (опц.) load
```

### 2.3 Состав моделей при обучении

```python
# в profile.yaml:
fit_args:
  hyperparameters:
    DeepAR: {}                          # только эти модели (+ ансамбль)
    TemporalFusionTransformer: {}
    ETS: {}
  # или исключить лишнее из дефолта:
  excluded_model_types: [Chronos2, AutoETS]
```

Совместимость с фичами (Model Zoo AutoGluon):
- static + known: `DeepAR`, `TemporalFusionTransformer`, `TiDE`, `WaveNet`,
  `DirectTabular`, `RecursiveTabular`;
- past covariates (цена): `TemporalFusionTransformer`, `Chronos2`;
- базовые (`Naive`, `SeasonalNaive`, `ETS`) фичи игнорируют — полезны как бенчмарк.

Рекомендуемый старт: `DeepAR + TemporalFusionTransformer + ETS`.

---

## 3. Вариант B: ручная сборка датасета (pandas, без пайплайна)

Подходит для экспериментов / новой задачи. Полный пример:

```python
import pandas as pd
from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor

# 1. сырые продажи: date, store, product, qty, promo, promo_type, price
raw = pd.read_parquet("data/04/r/sales_dataset.parquet")

# 2. item_id
raw["item_id"] = raw["LocationId"].astype(str) + "_" + raw["ItemId"].astype(str)

# 3. нормализация дат, сортировка
raw["date"] = pd.to_datetime(raw["TransDate"])
raw = raw.sort_values(["item_id", "date"])

# 4. относительная цена (инфляция/масштаб)
raw["PriceVsRollingMedian28d"] = (
    raw.groupby("item_id")["PriceRaw"]
    .transform(lambda s: s / s.rolling(28, min_periods=1).median() - 1)
)

# 5. промо → категориальная
raw["promo_type"] = raw["PromoTypeRaw"].fillna("none").astype("category")
raw["promo_flag"] = raw["PromoIsActiveRaw"].astype(int)

# 6. TimeSeriesDataFrame
tsdf = TimeSeriesDataFrame.from_data_frame(
    raw, id_column="item_id", timestamp_column="date"
)

# 7. static features (справочники)
static = catalog.merge(store_info, on="store_id")
static["item_id"] = static["LocationId"].astype(str) + "_" + static["ItemId"].astype(str)
for col in ["group", "subgroup", "brand", "network", "format", "region"]:
    static[col] = static[col].astype("category")
tsdf.static_features = static.set_index("item_id")

# 8. обучение
predictor = TimeSeriesPredictor(
    prediction_length=14,
    freq="D",
    target="target",
    known_covariates_names=["promo_flag", "promo_type"],
    eval_metric="WAPE",
).fit(tsdf, time_limit=3600)

# 9. прогноз (future = промо-план на горизонт)
future = predictor.make_future_data_frame(tsdf)
future["promo_flag"] = promo_plan  # подставить план на будущее
future["promo_type"] = promo_plan_type.astype("category")
preds = predictor.predict(tsdf, known_covariates=future)
```

---

## 4. Оценка результата

После `evaluate_model` в `data/04/r/`:

| Артефакт | Что смотреть |
|---|---|
| `leaderboard.yaml` | WAPE/score по каждой модели; лучшая — для predict |
| `feature_importance.yaml` | вклад фич (permutation) — убрать неинформативные |
| `autogluon_evaluation_metrics.yaml` | итоговая метрика на тесте |
| `prepared_ts_dataframe_meta.yaml` | shape, NaN-ratio по колонкам |
| `raw_predictions_ts.parquet` | сами прогнозы (квантили) |

Итерации:
1. смотреть `feature_importance` — отсекать мусор (`exclude_features`);
2. подбирать `prediction_length` и состав моделей (`hyperparameters`);
3. добавлять промо/цены/иерархии постепенно и сравнивать WAPE;
4. если рядов очень много — фильтр коротких рядов + сэмплирование
   (`filter_short_series`, `sample_items`).

---

## 5. Чек-лист

- [ ] item_id уникален для (магазин, товар); даты = datetime
- [ ] категории в static — dtype `category`/`object` (не int!)
- [ ] нулевые продажи явные (0), а не NaN; `freq="D"` задан
- [ ] промо-план на горизонт → `known_covariates_names`
- [ ] цена: `PriceVsRollingMedian28d`/`PriceChangePct` вместо сырой цены
- [ ] иерархии уровней — отдельные колонки, не схлопнуты
- [ ] длина рядов ≥ `max(prediction_length+1, 5) + prediction_length`
- [ ] static_features покрывает все item_id
- [ ] обучение: `hyperparameters` явно задан или `excluded_model_types`
- [ ] посмотреть leaderboard + feature_importance после первого прогона