job1resultDensity = read.csv("C:/Users/Nikola/Documents/R/job1/job1result.csv")

p1 <- ggplot(data=job1resultDensity, aes(x=Data, group=Prodotto, fill=Prodotto)) +
  geom_density(adjust=1.5, alpha=.4) +
  theme_ipsum()

p1

job1resultPlot = read.csv("C:/Users/Nikola/Documents/R/job1/job1result.csv")


p2 <- ggplot(data=job1resultPlot, aes(x=Data, y = Tot, color = Prodotto)) +
  geom_point(size = 6)+
  theme_ipsum()

p2

