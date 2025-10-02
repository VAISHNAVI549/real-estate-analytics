# Real Estate Analytics - R Visualization Script

required_packages <- c("ggplot2", "dplyr", "tidyr", "RPostgreSQL", "scales", "viridis", "gridExtra")

for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, repos='https://cloud.r-project.org', quiet = FALSE)
    library(pkg, character.only = TRUE)
  }
}

db_host <- "localhost"
db_port <- "5432"
db_name <- "real_estate_db"
db_user <- Sys.getenv("USER")
db_password <- ""

output_dir <- "../output/charts"
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

cat("Connecting to database...\n")
con <- dbConnect(
  PostgreSQL(),
  host = db_host,
  port = db_port,
  dbname = db_name,
  user = db_user,
  password = db_password
)

cat("Database connected successfully!\n\n")

theme_set(theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 16, hjust = 0.5),
    plot.subtitle = element_text(size = 12, hjust = 0.5, color = "gray30"),
    axis.title = element_text(face = "bold"),
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  ))

cat("Creating Chart 1: Rent vs Own Distribution...\n")

query_rent_own <- "
SELECT 
  sale_type,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM listings
WHERE sale_type IN ('sale', 'rent')
GROUP BY sale_type
"

df_rent_own <- dbGetQuery(con, query_rent_own)

if (nrow(df_rent_own) > 0) {
  df_rent_own$label <- paste0(
    tools::toTitleCase(df_rent_own$sale_type), 
    "\n", 
    scales::comma(df_rent_own$count),
    " (", df_rent_own$percentage, "%)"
  )
  
  p1 <- ggplot(df_rent_own, aes(x = "", y = count, fill = sale_type)) +
    geom_bar(stat = "identity", width = 1, color = "white", size = 2) +
    coord_polar("y", start = 0) +
    geom_text(aes(label = label), 
              position = position_stack(vjust = 0.5),
              size = 5, fontface = "bold", color = "white") +
    scale_fill_manual(values = c("rent" = "#3498db", "sale" = "#e74c3c")) +
    labs(
      title = "Rent vs Own Distribution",
      subtitle = "Property Listing Types",
      fill = "Type"
    ) +
    theme_void() +
    theme(
      plot.title = element_text(face = "bold", size = 18, hjust = 0.5),
      plot.subtitle = element_text(size = 12, hjust = 0.5, color = "gray40"),
      legend.position = "bottom",
      legend.title = element_text(face = "bold")
    )
  
  ggsave(
    filename = file.path(output_dir, "01_rent_vs_own_pie.png"),
    plot = p1,
    width = 10,
    height = 8,
    dpi = 300
  )
  
  cat("  Saved: 01_rent_vs_own_pie.png\n")
}

cat("Creating Chart 2: Property Type Comparison...\n")

query_property <- "
SELECT 
  property_type,
  COUNT(*) as count
FROM listings
WHERE property_type IN ('apartment', 'independent', 'condo')
  AND price IS NOT NULL
GROUP BY property_type
ORDER BY count DESC
"

df_property <- dbGetQuery(con, query_property)

if (nrow(df_property) > 0) {
  p2 <- ggplot(df_property, aes(x = reorder(property_type, -count), y = count, fill = property_type)) +
    geom_bar(stat = "identity", width = 0.7, color = "white", size = 1) +
    geom_text(aes(label = scales::comma(count)), 
              vjust = -0.5, size = 5, fontface = "bold") +
    scale_fill_viridis_d(option = "plasma", begin = 0.2, end = 0.8) +
    scale_y_continuous(labels = scales::comma, expand = expansion(mult = c(0, 0.1))) +
    labs(
      title = "Property Type Distribution",
      subtitle = "Apartments vs Independent Houses vs Condos",
      x = "Property Type",
      y = "Number of Listings",
      fill = "Type"
    ) +
    theme(
      axis.text.x = element_text(angle = 0, hjust = 0.5, size = 12),
      legend.position = "none"
    )
  
  ggsave(
    filename = file.path(output_dir, "02_property_type_bar.png"),
    plot = p2,
    width = 12,
    height = 8,
    dpi = 300
  )
  
  cat("  Saved: 02_property_type_bar.png\n")
}

cat("Creating Chart 3: Yearly Price Trends...\n")

query_yearly <- "
SELECT 
  EXTRACT(YEAR FROM date) as year,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
FROM listings
WHERE price IS NOT NULL
  AND date >= '2000-01-01'
GROUP BY EXTRACT(YEAR FROM date)
ORDER BY year
"

df_yearly <- dbGetQuery(con, query_yearly)

if (nrow(df_yearly) > 0) {
  p3 <- ggplot(df_yearly, aes(x = year, y = median_price)) +
    geom_line(color = "#2c3e50", size = 1.5) +
    geom_point(color = "#e74c3c", size = 3, shape = 21, fill = "white", stroke = 2) +
    scale_y_continuous(labels = scales::dollar_format(), expand = expansion(mult = c(0.05, 0.1))) +
    scale_x_continuous(breaks = scales::pretty_breaks(n = 10)) +
    labs(
      title = "Median House Price Trend",
      subtitle = "20+ Years of Real Estate Price Evolution",
      x = "Year",
      y = "Median Price (USD)"
    )
  
  ggsave(
    filename = file.path(output_dir, "03_yearly_price_trend_line.png"),
    plot = p3,
    width = 14,
    height = 8,
    dpi = 300
  )
  
  cat("  Saved: 03_yearly_price_trend_line.png\n")
}

dbDisconnect(con)
cat("\nAll visualizations created successfully!\n")
cat("Location:", output_dir, "\n")
