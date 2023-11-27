install.packages(c('dplyr','duckdb','altair'))
library(ggplot2)
library(dplyr)
library(duckdb)

db <- dbConnect(duckdb::duckdb())


info = dbGetQuery(db, "SELECT * FROM read_csv_auto('precios-en-surtidor-resolucin-3142016 (2).csv')
                        WHERE indice_tiempo =='2023-11' AND 
                            empresabandera == 'YPF' AND 
                            provincia == 'CAPITAL FEDERAL' AND 
                            producto == 'Nafta (súper) entre 92 y 95 Ron' AND
                            tipoHorario= 'Diurno'
                        ORDER BY precio DESC")

print(paste("La estacion de servicio mas cara con un precio de ", info$precio[1], " esta en la direccion: ", info$direccion[1]))

ggplot(info, aes(x=precio)) +
  geom_histogram(aes(), color="black") +
  labs(x="Precio", y="Count") +
  theme_minimal()
