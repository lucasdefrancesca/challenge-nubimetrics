SELECT cr.Name AS country_name,
	   c.Name AS currency_name,
	   CAST(MAX(cr2.AverageRate) AS DECIMAL(18, 2)) AS currency_rate,
	   CAST(AVG(str.TaxRate) AS DECIMAL(18, 2)) AS average_tax_rate
FROM Sales.CountryRegionCurrency crc
INNER JOIN Person.CountryRegion cr ON crc.CountryRegionCode = cr.CountryRegionCode
INNER JOIN Sales.Currency c ON crc.CurrencyCode = c.CurrencyCode
INNER JOIN Sales.CurrencyRate cr2 ON crc.CurrencyCode = cr2.ToCurrencyCode
INNER JOIN Person.StateProvince sp ON crc.CountryRegionCode = sp.CountryRegionCode
INNER JOIN Sales.SalesTaxRate str ON str.StateProvinceID = sp.StateProvinceID
GROUP BY cr.Name, c.Name
ORDER BY cr.Name
