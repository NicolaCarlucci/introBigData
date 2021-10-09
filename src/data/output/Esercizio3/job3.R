job3resultPlotAssociation = read.csv("C:/Users/Nikola/Documents/R/job3/job3resultSA.csv")

p3 <- ggplot(data=job3resultPlotAssociation, aes(x=SupportoAssociazione, y = ConfidenzaAssociazione, color = Coppia)) +
  geom_point(size =6)+
theme_ipsum()

p3


job3resultPlotConfident = read.csv("C:/Users/Nikola/Documents/R/job3/job3resultSC.csv")

p4 <- ggplot(data=job3resultPlotConfident, aes(x=SupportoAssociazione, y = ConfidenzaAssociazione, color = Coppia)) +
  geom_point(size =6)+
  theme_ipsum()

p4

