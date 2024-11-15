\connect reservations program

INSERT INTO hotels (
    id, 
    hotel_uid, 
    name, 
    country, 
    city, 
    address, 
    stars, 
    price
) 
VALUES (
    1, 
    '049161bb-badd-4fa8-9d90-87c9a82b0668', 
    'Ararat Park Hyatt Moscow', 
    'Россия', 
    'Москва', 
    'Неглинная ул., 4', 
    5, 
    10000
);

\connect loyalties program

INSERT INTO loyalty (
    id, 
    username, 
    reservation_count, 
    status, 
    discount
) 
VALUES (
    1, 
    'Test Max', 
    25,
    'GOLD', 
    10
);