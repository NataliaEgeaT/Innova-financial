1. Introducción

El siguiente documento describe el proceso de diseño del modelo dimensional implementado para el Data Warehouse financiero de Innova. El modelo fue construido siguiendo las mejores prácticas de Kimball, utilizando un enfoque orientado al análisis de métricas SaaS como:

- MRR (Monthly Recurring Revenue)
- CAC (Customer Acquisition Cost)
- Free Cash Flow (FCF)
- Ingresos por país y por cliente
- Evolución de suscripciones

2. Decisiones de Diseño
Se seleccionó un esquema en estrella por las siguientes razones:

- Permite consultas analíticas much más eficientes.
- Facilita la creación de métricas complejas con SQL simple.
- Escala correctamente cuando se agregan más países, monedas o años.
- Se ajusta a las necesidades del equipo financiero (MRR, FCF, gastos, crecimiento).
- Es compatible con dbt y herramientas BI como Looker Studio o Power BI.

El diagrama muestra un modelo altamente normalizado en dimensiones y claramente orientado a hechos transaccionales (ver diagrama en el PDF adjunto Star_schema.pdf)

3. Definición de Granularidad de Hechos

La granularidad es la decisión más importante en un DW porque define qué representa cada fila.
A continuación, la granularidad formal de cada fact table.

    3.1 fact_revenue — Granularidad: una transacción individual

    Cada fila representa una transacción de ingreso registrada para un cliente en una fecha específica.

    Justificación:

    - Las fuentes incluyen transactions.csv, donde cada transacción es un evento independiente.
    - Finanzas necesita analizar ingresos por país, cliente, fecha y canal.
    - Permite calcular ingresos brutos, netos, impuestos y revenue por cohortes.

    Claves:
    - customer_key
    - date_key
    - country_key
    - transaction_id

    3.2 fact_subscriptions_mrr — Granularidad: snapshot mensual de suscripciones

    Cada fila representa el valor MRR de una suscripción específica durante un mes.

    Justificación:
    - El MRR es una métrica derivada, no una transacción.
    - La granularidad mensual sigue los estándares SaaS.
    - Permite calcular churn, upgrades, downgrades, expansión y contracción.
    - Se basa en subscriptions.csv.

    Claves:
    - date_key
    - customer_key
    - plan_key

    3.3 fact_expenses — Granularidad: un gasto único

    Cada fila en esta tabla representa un gasto operativo individual.

    Justificación:

    - Necesario para calcular CAC y FCF.
    - Permite separar Marketing vs. Operaciones mediante dim_expense_category.
    - Granularidad simple = fácil acumulación mensual o anual.

    Claves:
    - date_key
    - employee_key (si aplica)
    - category_key

4. Dimensiones Requeridas y Justificación

Las siguientes dimensiones fueron identificadas como imprescindibles para soportar las métricas del negocio.

El modelo se observa visualmente en el PDF adjunto donde cada una se relaciona a las tablas de hechos de forma correcta (página 1) .

    4.1 dim_date

    Incluye atributos como:
    - día
    - mes
    - trimestre
    - año
    - nombre del mes
    - indicador de fin de semana

    Justificación:

    - Permite análisis por períodos, cohortes, estacionalidad y cortes financieros.
    - Ideal para funciones analíticas en SQL y BI.

    4.2 dim_country

    Representa el país donde ocurre:
    - una transacción
    - una suscripción
    - un gasto
    - un cliente
    - un empleado

    Justificación:

    - Finanzas requiere visibilidad por país.
    - Escalabilidad: agregar nuevos países no afecta las tablas de hechos.

    4.3 dim_customer

    Contiene nombre del cliente, país, canal y fecha de creación.

    Justificación:
    - Métrica “nuevos clientes por mes” depende de esta dimensión.
    - Permite calcular CAC por canal.
    - Permite cohortes de ingreso y churn.

    4.4 dim_channel

    Lista de canales de adquisición.

    Justificación:
    - Necesaria para CAC.
    - Normaliza valores que vienen de texto libre en los CSV.

    4.5 dim_plan

    Representa planes SaaS, precios y frecuencia.

    Justificación:
    - Core del cálculo de MRR.
    - Permite análisis por tipo de plan.

    4.6 dim_employee

    Incluye empleados involucrados en gastos o nómina.

    Justificación:
    - Algunas empresas imputan gastos de personal a FCF.
    - Soporta análisis de payroll por país y rol.

    4.7 dim_expense_category

    Clasifica los gastos en:
    - Marketing
    - Operación
    - Tecnología
    - Administración

    Justificación:
    - Clave para distinguir CAC y OPEX.
    - Permite agrupar gastos en dashboards.


5. Justificación Global del Diseño

El diseño final del modelo:

✔ Extrae sus entidades directamente desde los 6 CSV provistos
✔ Sigue las reglas de modelado dimensional de Kimball
✔ Mantiene lógica analítica clara: facts = eventos / cálculos, dims = contexto
✔ Deja las métricas derivadas (MRR, FCF, CAC) para la capa analítica, no para almacenamiento
✔ Reduce duplicación y mejora rendimiento en BI
✔ Soporta escalabilidad a nuevos países y nuevos planes
✔ Está estructurado para integrarse con dbt, Airflow, DuckDB/Postgres o Athena