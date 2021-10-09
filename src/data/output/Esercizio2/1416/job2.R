job2resultPlot = read.csv("C:/Users/Nikola/Documents/R/job2/job2result.csv")

p2 <- ggplot(data=job2resultPlot, aes(x=Data, y = Tot, color = Prodotto)) +
  geom_point(size = 1)+ geom_rug()
  theme_ipsum()

p2


p <- ggplot(job2resultPlot, aes(x=Tot)) + 
  geom_histogram()
p

