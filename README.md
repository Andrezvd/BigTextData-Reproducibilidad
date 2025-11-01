# Challenge de Reproducibilidad — Efficient Big Text Data Clustering using Hadoop and Spark

## Descripción General

Este proyecto corresponde al *Challenge de Reproducibilidad* de la asignatura de Análisis de Datos a Gran Escala.  
El objetivo es **reproducir desde cero** la implementación del artículo científico:

**"Efficient Big Text Data Clustering Algorithms using Hadoop and Spark"**  

El reto busca evaluar la capacidad del equipo para **comprender, documentar y replicar** procesos complejos de análisis de datos de manera **precisa y transparente**, aplicando buenas prácticas en programación reproducible, control de versiones y despliegue en entornos distribuidos.

---

## Metodología de Trabajo

La metodología se basa en tres etapas principales que garantizan **reproducibilidad, portabilidad y escalabilidad** del desarrollo:

### 1. Desarrollo y pruebas locales

Durante esta etapa se realiza todo el trabajo de desarrollo y validación inicial en entorno local, sin depender del clúster institucional.

- **Lenguaje principal:** Python 3.10  
- **Frameworks:** Hadoop Streaming y PySpark  
- **Entorno de ejecución:** Visual Studio Code + Conda
- **Objetivo:** validar el funcionamiento de los algoritmos en datasets reducidos.

#### Pasos clave:
1. Implementar los módulos base:
   - `mapper.py` y `reducer.py` (para Hadoop MapReduce local).
   - `clustering_spark.py` (para la versión paralela en PySpark).
2. Probar localmente con datos de ejemplo:
   ```bash
   cat data/sample_text.txt | python3 src/mapreduce/mapper.py | sort | python3 src/mapreduce/reducer.py
   spark-submit --master local[2] src/spark/clustering_spark.py

3. ### Simulación de entorno distribuido con Docker

Antes de ejecutar en el clúster real, se replica el entorno Hadoop + Spark usando contenedores Docker.

Objetivo: probar la compatibilidad del código con entornos distribuidos sin salir del entorno local.

Herramientas: Docker y Docker Compose.

Componentes simulados: Spark Master, Spark Worker, Hadoop Namenode y Datanode.

4. ### Despliegue final en el clúster UIS

Una vez validadas las pruebas locales y en Docker, el código se migra al clúster de cómputo de la Universidad Industrial de Santander.

Entorno real: Hadoop/Spark en modo distribuido (YARN).

Objetivo: ejecutar el clustering de texto sobre datasets grandes y obtener métricas reales de desempeño.

hdfs dfs -put data/large_text_dataset.txt /user/data/

### Estructura del Proyecto
```bash
bigtext-hadoop-spark-clustering/
│
├── data/                     # Datasets de prueba (pequeños para local)
│   ├── sample_texts.txt
│
├── src/                      # Código fuente
│   ├── mapreduce/            # Algoritmos MapReduce en Python o Java
│   │   ├── mapper.py
│   │   ├── reducer.py
│   │   └── run_local.sh
│   │
│   ├── spark/                # Algoritmos Spark en Python (PySpark)
│   │   ├── clustering_spark.py
│   │   └── run_local.sh
│
├── notebooks/                # Pruebas y exploraciones Jupyter
│   └── EDA_big_text.ipynb
│
├── docs/                     # Documentación del proyecto
│   ├── informe.pdf
│   ├── referencias.bib
│   └── presentacion.pptx
│
├── environment.yml           # Dependencias Conda/Python
├── docker-compose.yml        # Para entorno Hadoop + Spark local
├── Dockerfile
├── README.md
└── .gitignore

```

Autor

Andrés Olivar
Estudiante de Ingeniería de Sistemas — Universidad Industrial de Santander
Bucaramanga, Colombia