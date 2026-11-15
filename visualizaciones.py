import matplotlib.pyplot as plt
from spark_eda import ejecutar_eda

def generar_graficos():
    spark, pdf_minutos, pdf_categorias, pdf_temporal = ejecutar_eda()

    try:
        # Gráfico 1: Distribución de minutos reproducidos
        plt.figure(figsize=(10, 6))

        plt.hist(
            pdf_minutos["minutos_reproducidos"],
            bins=15,
            edgecolor="black"
        )

        plt.title("Distribución de minutos reproducidos")
        plt.xlabel("Minutos reproducidos")
        plt.ylabel("Frecuencia de reproducciones")
        plt.tight_layout()
        plt.savefig("grafico_descriptivo.png")
        plt.close()

        print("\nInterpretación gráfico 1:")
        print("El histograma muestra con que frecuencia se registran distintos tiempos de reproducción.")


        #Gráfico 2: Promedio por tipo de contenido

        plt.figure(figsize=(10, 6))

        plt.bar(
            pdf_categorias["tipo_contenido"],
            pdf_categorias["promedio_minutos"],
            edgecolor="black"
        )

        plt.title("Promedio de minutos reproducidos por tipo de contenido")
        plt.xlabel("Tipo de contenido")
        plt.ylabel("Promedio de minutos reproducidos")
        plt.tight_layout()
        plt.savefig("grafico_categorias.png")
        plt.close()

        categoria_mayor = pdf_categorias.loc[
            pdf_categorias["promedio_minutos"].idxmax()
        ]

        print("\nInterpretación gráfico 2:")
        print(
            f"El contenido con mayor promedio de reproducción es "
            f"{categoria_mayor['tipo_contenido']}, "
            f"con {categoria_mayor['promedio_minutos']:.2f} minutos."
        )

        # Gráfico 3: Reproducciones por hora

        plt.figure(figsize=(10, 6))

        plt.plot(
            pdf_temporal["hora"],
            pdf_temporal["cantidad_reproducciones"],
            marker="o"
        )

        plt.title("Cantidad de reproducciones por hora del día")
        plt.xlabel("Hora del día")
        plt.ylabel("Cantidad de reproducciones")
        plt.xticks(range(24))
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig("grafico_temporal.png")
        plt.close()

        hora_mayor = pdf_temporal.loc[
            pdf_temporal["cantidad_reproducciones"].idxmax()
        ]

        print("\nInterpretación gráfico 3:")
        print(
            f"La mayor cantidad de reproducciones se registró "
            f"a las {int(hora_mayor['hora'])}:00 horas, "
            f"con {int(hora_mayor['cantidad_reproducciones'])} reproducciones."
        )

        print("\nGráficos generados correctamente!!")
        print("Archivos creados:")
        print("grafico_descriptivo.png")
        print("grafico_categorias.png")
        print("grafico_temporal.png")

    finally:
        spark.stop()

if __name__ == "__main__":
    generar_graficos()