library(dplyr)
library(ggplot2)
library(tidyverse)
library(lubridate)
library(wordcloud)
library(scales)
library(arules)
library(arulesViz)
library(factoextra)

#------------------------EDA----------------------------------

online_retail <- Online_Retail

str(online_retail)

#What is the return rate for products and which products have the highest return rates?
returns <- online_retail %>% filter(Quantity < 0)
return_rate <- nrow(returns) / nrow(online_retail) * 100
return_rate
return_rates_by_product <- returns %>% 
  group_by(StockCode, Description) %>% 
  summarize(ReturnRate = n() / abs(sum(Quantity)) * 100, .groups = "drop_last") %>% 
  arrange(desc(ReturnRate)) %>% 
  slice(1:10)
return_rates_by_product <- return_rates_by_product %>% na.omit()
return_rates_by_product <- return_rates_by_product %>% arrange(desc(ReturnRate))
return_rates_by_product

online_retail
# Clean the dataset by removing missing values, negative quantities and prices, and transactions with no customer ID
online_retail <- online_retail %>%
  drop_na() %>%
  filter(Quantity > 0, UnitPrice > 0, CustomerID != "NA")

# Remove missing values
online_retail <- online_retail[complete.cases(online_retail),]

# Removing duplicates: Remove duplicate rows
online_retail <- unique(online_retail)

# Add a TotalPrice column by multiplying Quantity and UnitPrice
online_retail <- online_retail %>% mutate(TotalPrice = Quantity * UnitPrice)
view(online_retail)

# Data type conversion:CustomerID to integer format
online_retail$CustomerID <- as.integer(online_retail$CustomerID)

# Handling outliers: Remove rows with extreme values in Quantity or UnitPrice
online_retail <- online_retail %>%
  filter(Quantity >= 0 & Quantity <= quantile(Quantity, 0.99),
         UnitPrice >= 0 & UnitPrice <= quantile(UnitPrice, 0.99))

# Handling inconsistent data: Standardize the formatting of text in the Description column
online_retail$Description <- gsub("[[:punct:]]", "", online_retail$Description) # Remove punctuation
online_retail$Description <- tolower(online_retail$Description) # Convert to lowercase

# Explore the distribution of TotalPrice
summary(online_retail$TotalPrice)
hist(online_retail$TotalPrice)

# Explore the distribution of Quantity
summary(online_retail$Quantity)
hist(online_retail$Quantity)

# Explore the distribution of UnitPrice
summary(online_retail$UnitPrice)
hist(online_retail$UnitPrice)

# Handling data skewness: Apply log transformation to the Quantity and UnitPrice columns
online_retail$Quantity <- log(online_retail$Quantity + 1)
online_retail$UnitPrice <- log(online_retail$UnitPrice + 1)


#-------------------------------Visualization and Insights--------------------------------

# Plot 1: How do sales vary by month?

sales_by_month <- online_retail %>%
  mutate(Month = floor_date(InvoiceDate, "month")) %>%
  group_by(Month) %>%
  summarize(TotalSales = sum(TotalPrice))
ggplot(sales_by_month, aes(x = Month, y = TotalSales)) +
  geom_line() +
  labs(x = "Month", y = "Total Sales", title = "Sales by Month")


# Plot 2: What are the top-selling products and how much revenue do they generate?
sales_by_product <- online_retail %>%
  group_by(StockCode, Description) %>%
  summarize(TotalSales = sum(TotalPrice), .groups = "drop")

sales_by_product <- sales_by_product[order(sales_by_product$TotalSales, decreasing = TRUE),]

head(sales_by_product, 10)
top_products <- head(sales_by_product, 10)

ggplot(top_products, aes(x = fct_reorder(Description, TotalSales), y = TotalSales)) +
  geom_col(fill = "#0072B2") +
  scale_y_continuous(labels = dollar_format(prefix = "$", scale = 1e-6, suffix = "M")) +
  coord_flip() +
  labs(x = "Product Description", y = "Total Sales", 
       title = "Top-Selling Products and Revenue Generated", 
       subtitle = "Sales by product from January 2010 to December 2011") +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold", size = 16),
        plot.subtitle = element_text(size = 14),
        axis.text = element_text(size = 12),
        axis.title = element_text(size = 14, face = "bold"),
        panel.grid.major.x = element_blank())


# Plot 3: What is the trend of total sales over time?
sales_by_quarter <- online_retail %>%
  mutate(Quarter = quarter(InvoiceDate)) %>%
  group_by(Quarter) %>%
  summarize(TotalSales = sum(TotalPrice))


ggplot(sales_by_quarter, aes(x = Quarter, y = TotalSales, fill = Quarter)) +
  geom_bar(stat = "identity", color = "black") +
  geom_text(aes(label = paste0("$", format(TotalSales, big.mark = ","))), 
            vjust = -0.5, size = 3.5) +
  scale_y_continuous(labels = scales::dollar_format(scale = 1e-6)) +
  xlab("Quarter") +
  ylab("Total Sales (in millions of dollars)") +
  ggtitle("Total Sales by Quarter") +
  theme_classic()


# Plot 4: Which customers generated the most revenue and how much did they contribute to the total revenue?
top_customers <- online_retail %>% 
  group_by(CustomerID) %>% 
  summarize(TotalSales = sum(TotalPrice)) %>% 
  arrange(desc(TotalSales)) %>% 
  slice(1:10) %>% 
  mutate(PercentageOfTotal = TotalSales / sum(TotalSales) * 100,
         CustomerID = factor(CustomerID))

ggplot(top_customers, aes(x = "", y = PercentageOfTotal, fill = CustomerID)) +
  geom_bar(width = 1, stat = "identity") +
  coord_flip() +
  scale_fill_viridis_d() +
  labs(x = NULL, y = NULL, fill = "Customer ID") +
  ggtitle("Top Customers and Their Contribution to Total Revenue") +
  theme_minimal() +
  theme(legend.position = "bottom")


# Plot 5: How do sales vary by season?
get_season <- function(month) {
  ifelse(month %in% c(3, 4, 5), "Spring",
         ifelse(month %in% c(6, 7, 8), "Summer",
                ifelse(month %in% c(9, 10, 11), "Fall",
                       "Winter")))
}
online_retail$InvoiceMonth <- as.numeric(format(online_retail$InvoiceDate, "%m"))
online_retail$Season <- get_season(online_retail$InvoiceMonth)
sales_by_season <- online_retail %>%
  group_by(Season) %>%
  summarize(TotalSales = sum(TotalPrice)) %>%
  mutate(Percentage = scales::percent(TotalSales/sum(TotalSales)))

ggplot(sales_by_season, aes(x = "", y = TotalSales, fill = Season)) +
  geom_bar(stat = "identity", width = 1) +
  coord_polar("y", start = 0) +
  scale_fill_manual(values = c("Spring" = "#7fc97f", "Summer" = "#beaed4", "Fall" = "#fdc086", "Winter" = "#ffff99")) +
  theme_void() +
  labs(title = "Sales by Season") +
  theme(plot.title = element_text(hjust = 0.5, size = 24),
        legend.position = "bottom",
        legend.title = element_blank(),
        panel.spacing = unit(1, "lines"),
        axis.line = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank()) +
  geom_label(aes(label = Percentage), position = position_stack(vjust = 0.5), show.legend = FALSE)



# Plot 6: what are the top products across years?

online_retail$InvoiceDate <- as.POSIXct(online_retail$InvoiceDate, format="%Y-%m-%d %H:%M:%S")

# Create a new column for the year of each transaction
online_retail$Year <- format(online_retail$InvoiceDate, "%Y")

# Group the transactions by year and product, and calculate the total sales revenue for each product
product_sales <- online_retail %>%
  group_by(Year, Description) %>%
  summarise(TotalRevenue = sum(Quantity * UnitPrice))

# Get the top 10 products by total sales revenue across all years
top_products <- product_sales %>%
  group_by(Description) %>%
  summarise(TotalSales = sum(TotalRevenue)) %>%
  ungroup() %>%
  arrange(desc(TotalSales)) %>%
  head(10) %>%
  inner_join(product_sales, by = "Description")

# Create a bar chart showing the top 10 products by total sales revenue across years
ggplot(top_products, aes(x = Year, y = TotalRevenue, fill = Description)) +
  geom_col(position = "dodge") +
  labs(title = "Top Products by Total Sales Revenue Across Years", x = "Year", y = "Total Sales Revenue", fill = "Product Name") +
  theme_minimal()


# Plot 7: What is the daily sales trend?

online_retail_daily <- online_retail %>%
  group_by(Date = as.Date(InvoiceDate)) %>%
  summarize(TotalSales = sum(TotalPrice))

ggplot(online_retail_daily, aes(Date, TotalSales)) +
  geom_line() +
  labs(title = "Daily Sales Trend",
       x = "Date", y = "Total Sales")

# Plot 8: What are the top 5 countries with the highest number of purchases?

online_retail <- online_retail %>% filter(Quantity > 0)# Filter out negative Quantity values
# Count the number of purchases per country
country_counts <- online_retail %>%
  filter(!is.na(Country)) %>%
  group_by(Country) %>%
  summarise(num_purchases = n_distinct(InvoiceNo))
# Order the countries by the number of purchases
top_countries <- country_counts %>%
  arrange(desc(num_purchases)) %>%
  top_n(5)
#Bar plot of the top 5 countries
ggplot(top_countries, aes(x = reorder(Country, num_purchases), y = num_purchases)) +
  geom_bar(stat = "identity", fill = "blue") +
  labs(title = "Top 5 Countries with the Highest Number of Purchases",
       x = "Country", y = "Number of Purchases")


# Plot 9: What are the top productes that were purchased in UK?

# Filter data to include only UK transactions
data_uk <- subset(online_retail, Country == "United Kingdom")

# Get the top products sold in the UK
top_products <- data_uk %>%
  group_by(Description) %>%
  summarise(total_quantity = sum(Quantity)) %>%
  arrange(desc(total_quantity)) %>%
  head(n = 5)

# View the top products
top_products

# Create a bar plot of the top products
ggplot(data = top_products, aes(x = Description, y = total_quantity)) +
  geom_bar(stat = "identity", fill = "blue") +
  xlab("Product Description") +
  ylab("Total Quantity Sold") +
  ggtitle("Top 5 Products Sold in the UK")

#---------------------------Text Analysis------------------------------------
#This analysis is to identify what words are the most used in the product description attribute 
library(tm)
library(wordcloud)

corpus <- Corpus(VectorSource(online_retail$Description))
corpus <- tm_map(corpus, content_transformer(tolower))
corpus <- tm_map(corpus, removeNumbers)
corpus <- tm_map(corpus, removePunctuation)
corpus <- tm_map(corpus, removeWords, stopwords("english"))
tdm <- TermDocumentMatrix(corpus)
m <- as.matrix(tdm)
word_freq <- sort(rowSums(m), decreasing = TRUE)
wordcloud(names(word_freq), word_freq, max.words = 200, colors = brewer.pal(8, "Dark2"))

#------------------------Market Basket Analysis------------------------------------
library(data.table)#

online_retail$InvoiceNo <- as.factor(online_retail$InvoiceNo)
online_retail$Description <- as.factor(online_retail$Description)
# Create transactions
transactions <- split(online_retail$Description, online_retail$InvoiceNo)
transactions <- as(transactions, "transactions")
# Generate frequent itemsets
frequent_itemsets <- eclat(transactions, parameter = list(support = 0.05))
rules <- apriori(transactions, parameter = list(support = 0.02, confidence = 0.5))
inspectDT(frequent_itemsets)
inspectDT(rules)

sub_rules <- subset(rules, lift > 2)
top_rules <- head(sort(sub_rules, by="lift", decreasing=TRUE), n=10)
inspectDT(top_rules)

# Visualize
plot(top_rules)
plot(top_rules,method="graph",asEdges=TRUE,limit = 5)
#------------------------------------------Clustering-------------------------------------------

## First I will add the recency, monetary value of purchases, and frequency of each customers

online_retail$CustomerID<-as.factor(online_retail$CustomerID)
date<-as.Date("2011-12-10")

online_retail_cust <- online_retail %>% group_by(CustomerID) %>% 
  summarize(amount_spent=sum(TotalPrice), purchase_frequency=n(), quantity_puchased=sum(Quantity),
            avg_unit_price=mean(UnitPrice), days_last_purchase=round(as.numeric(difftime(date,max(InvoiceDate))),digits=0))


## Hierarchical Clustering

# calculate the distance matrix (euclidean)
online_retail_cust2<-scale(online_retail_cust[c(2:6)]) 

#correlations
cor(online_retail_cust2)
pairs(online_retail_cust2)

distance=dist(online_retail_cust2)
retail_cust.hcluster<-hclust(distance)


#characterize clusters based on cutree
hcluster=cutree(retail_cust.hcluster,4)

# tabulate membership
table(hcluster)

#Look at charactersitics by cluster
aggregate(online_retail_cust2,list(hcluster),mean)

# find optimal numbers
fviz_nbclust(online_retail_cust2,kmeans,method="wss")

#silhouette
fviz_nbclust(online_retail_cust2,kmeans,method="silhouette")
fviz_nbclust(online_retail_cust2,FUN=hcut,method="silhouette")

## KMeans Clustering
set.seed(123)
cluster.cust<-kmeans(online_retail_cust2,3,nstart=25)

# Looks like KMeans has a decent silhouette score, so I'll stick with four clusters with KMeans
#visualizing and objective measures
fviz_cluster(cluster.cust,online_retail_cust2)

# Applying clusters to the customer data
online_retail_cust$cluster<-cluster.cust$cluster

#Examine Characteristics of Clusters
summary(online_retail_cust[online_retail_cust$cluster==1,])
summary(online_retail_cust[online_retail_cust$cluster==2,])
summary(online_retail_cust[online_retail_cust$cluster==3,])