-- the old text_pattern_ops index is not useful for our only query which uses something like:
-- select 1 from tabular where '123' like location || '%';
-- so we drop it and create a new btree index instead that we'll use for exact match queries
drop index tabular_location_idx;

create index tabular_location_idx on tabular (location);


CREATE OR REPLACE FUNCTION check_tabular_location_uniqueness()
    RETURNS trigger
    LANGUAGE plpgsql
AS
$$
DECLARE
    split_point     TEXT;
    prefix          TEXT := '';
    split_points    TEXT[];
    protocol_prefix TEXT := '';
BEGIN
    protocol_prefix := substring(NEW.location FROM '^[a-zA-Z0-9]+://');
    NEW.location := rtrim(NEW.location, '/') || '/';
    split_points := string_to_array(substring(NEW.location FROM '^[a-zA-Z0-9]+://(.*)$'), '/');

    FOREACH split_point IN ARRAY split_points
        LOOP
            IF split_point != '' THEN
                prefix := prefix || split_point || '/';
                -- Check if the prefix is in any existing location
                IF EXISTS (SELECT 1
                           FROM tabular
                           WHERE location = protocol_prefix || prefix
                             AND tabular_id != NEW.tabular_id) THEN
                    RAISE unique_violation USING MESSAGE = 'NEW.location cannot share a prefix with another location';
                END IF;
            END IF;
        END LOOP;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER tabular_location_uniqueness
    BEFORE INSERT OR UPDATE
    ON tabular
    FOR EACH ROW
EXECUTE FUNCTION check_tabular_location_uniqueness();

