CREATE TABLE Sales(
    SalesId INT NOT NULL AUTO_INCREMENT, 
    InvoiceId INT NOT NULL, 
    CustomerId INT NOT NULL, 
    AlbumId INT NOT NULL,
    ArtistID INT NOT NULL,
    InvoiceDate DATETIME,
    Total DECIMAL(10,2),
    PRIMARY KEY(SalesId),
    FOREIGN KEY (InvoiceId) REFERENCES Invoice(InvoiceId),
    FOREIGN KEY (CustomerId) REFERENCES Customer(CustomerId),
    FOREIGN KEY (AlbumId) REFERENCES Album(AlbumId),
    FOREIGN KEY (ArtistID) REFERENCES Artist(ArtistID)
);