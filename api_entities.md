# Описание сущностей для витрины расчетов с курьерами

## Список таблиц в слое STG (загрузка из API)

- `stg.api_couriers`
  - `id` - идентификатор записи
  - `courier_id` - ID курьера
  - `object_value` - JSONB объект с данными курьера

- `stg.api_deliveries`
  - `id` - идентификатор записи
  - `delivery_id` - ID доставки
  - `object_value` - JSONB объект с данными доставки

- `stg.api_restaurants`
  - `id` - идентификатор записи
  - `restaurant_id` - ID ресторана
  - `object_value` - JSONB объект с данными ресторана

## Список таблиц в слое DDS

- `dds.dm_couriers`
  - `id` - идентификатор записи
  - `courier_id` - ID курьера
  - `courier_name` - имя курьера

- `dds.dm_deliveries`
  - `id` - идентификатор записи
  - `delivery_id` - ID доставки
  - `address` - адрес доставки
  - `delivery_ts` - дата и время доставки

- `dds.dm_delivery_details`
  - `id` - идентификатор записи
  - `delivery_id` - ID доставки
  - `courier_id` - ID курьера
  - `order_id` - ID заказа

- `dds.fct_deliveries`
  - `id` - идентификатор записи
  - `delivery_id` - ID доставки
  - `rate` - рейтинг доставки
  - `sum` - сумма заказа
  - `tip_sum` - сумма чаевых

## Список таблиц в слое CDM

- `cdm.fct_courier_settlement_report`
  - `id` - идентификатор записи
  - `courier_id` - ID курьера
  - `courier_name` - Ф.И.О. курьера
  - `settlement_year` - год отчета
  - `settlement_month` - месяц отчета
  - `orders_count` - количество заказов за период
  - `orders_total_sum` - общая сумма заказов
  - `order_processing_fee` - сумма удержанная компанией за обработку заказов
  - `courier_order_sum` - сумма, которую необходимо перечислить курьеру
  - `courier_reward_sum` - итоговая сумма к перечислению курьеру
  - `courier_tips_sum` - сумма чаевых курьеру
  - `rate_avg` - средний рейтинг курьера
  - `CONSTRAINT fct_courier_settlement_report_unique_courier_date UNIQUE (courier_id, settlement_year, settlement_month)`