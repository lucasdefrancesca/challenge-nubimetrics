SELECT sp.CountryRegionCode,
	   AVG(str.TaxRate) AS average_taxRate 
FROM Sales.SalesTaxRate str  
INNER JOIN Person.StateProvince sp ON str.StateProvinceID = sp.StateProvinceID 
GROUP BY sp.CountryRegionCode
