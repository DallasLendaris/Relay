-- Data minimization (audit PRIV-5, remainder): the server keeps no house number.
--
-- New rows already get a coarse short_address ("Elm St, San José"; see geo/coarse-address.ts).
-- This strips the house number from rows saved before that change. BEST EFFORT: it removes a
-- leading run of digits with an optional letter or hyphenated suffix, followed by a space
-- ("1600 ", "12-B ", "221B "). It does not catch spelled-out numbers ("One Market St") or a
-- number that is not at the start; those rows keep their text until the place is deleted.
-- Ordinals such as "1st St" and "5th Ave" are left alone (no space after the digits and letter).
-- Safe to run twice: after one pass no value starts with a number, so a second pass matches nothing.
UPDATE places
SET short_address = btrim(regexp_replace(short_address, '^[0-9]+([A-Za-z]|-[0-9A-Za-z]+)? +', ''))
WHERE short_address ~ '^[0-9]+([A-Za-z]|-[0-9A-Za-z]+)? +';

-- A value that was only a number becomes "no street text".
UPDATE places SET short_address = NULL WHERE short_address IS NOT NULL AND btrim(short_address) = '';
