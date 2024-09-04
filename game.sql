-- Copyright (c) 2024 nuno-faria
--
-- This software is released under the MIT License.
-- https://opensource.org/licenses/MIT


--- Prerequisites ---

-- Table to store the inputs. cmd stores the key; ts stores the timestamp when the key was pressed.
-- It is created outside of the query to make it available to the user.
-- Marking it as "UNLOGGED" means it is not logged to the WAL, making writes faster.
-- Possible cmd values:
--   'u' - rotate, 'd' - move down, 'l' - move left, 'r' - move right, 's' - full drop, 'p' - pause
CREATE UNLOGGED TABLE IF NOT EXISTS Input (cmd char, ts timestamp);
TRUNCATE Input;
INSERT INTO Input VALUES ('', now());


-- Function to write a string to the console/log, using Postgres' RAISE command. The command is not
-- available in regular SQL code, so this is the one time where plpgsql must be used. This function
-- is necessary to render the game since Postgres only starts showing a recursive CTE's output when
-- it is fully completed.
CREATE OR REPLACE FUNCTION notify(str varchar) RETURNS void AS $$
BEGIN
    RAISE NOTICE '%', str;
END
$$ LANGUAGE PLPGSQL;


-- dblink is a Postgres extension that allows querying a remote database. In this case, it is used
-- to connect to the current database and read the most up to date information in the Input table,
-- which would otherwise not be possible. While Postgres supports Read Committed (default isolation)
-- for transactions, only a single query is executed here. For single queries, the isolation
-- behavior adheres to Snapshot semantics, meaning reads to the Input table will be the same as if
-- they were executed at the start of the query.
CREATE EXTENSION IF NOT EXISTS dblink;


--- Tetris game query ---

-- Main game loop implemented with a recursive Common Table Expression (CTE) query. The loop is
-- designed to run until a piece clashes with another on spawn (i.e., game over).
WITH RECURSIVE main AS (
    -- Constant parameters
    WITH const AS (
        SELECT
            -- board width
            10 AS width,
            -- board height
            20 AS height,
            -- frames per second the game loop runs at
            60 AS fps,
            -- initial interval at which a piece drops one line, i.e., gravity (seconds)
            48/60.0 AS init_drop_delta,
            -- minimum interval between piece drops (seconds)
            6/60.0 AS min_drop_delta,
            -- amount to decrease the drop interval per each level (seconds)
            2/60.0 AS drop_delta_decrease,
            -- number of lines to clear to increase one level
            10 AS lines_per_level,
            -- weight given to the current level in the earned points, according to the formula:
            -- base_points * (max(1, level * level_score_multiplier)). when set to 0, the level has
            -- no weight on the points earned
            1 AS level_score_multiplier
    ),
    -- Number of points awarded based on the number of lines cleared in the same move (base_points)
    points_per_line(lines, points) AS (
        SELECT *
        FROM (
            VALUES
                (0, 0),
                (1, 100),
                (2, 300),
                (3, 500),
                (4, 800)
        ) _
    ),
    -- Set of pieces/tetrominoes
    --   id identifies a piece, without rotation, from 0 to 6
    --   rotation defines a rotation of a piece, from 0 to 3 (based on Nintendo Rotation System)
    --   piece is an array storing the initial coordinates of a piece in the board. the board
    --     coordinates are represented by a sequential integer from 0 to (const.width + 1) *
    --     const.height, where 0 is the cell at the top-left corner (each level actually has
    --     const.width + 1 cells, more details later).
    --     for example, [4, 5, (const.width+1) + 4, (const.width+1) + 5] represents a square piece
    --     in the middle of the first and second lines of the board.
    tetromino(id, rotation, piece) AS (
        SELECT id, rotation, piece
        FROM const c(w), LATERAL (
            VALUES
                -- O
                (0, 0, ARRAY[4, 5, (c.w+1) + 4, (c.w+1) + 5]),
                (0, 1, ARRAY[4, 5, (c.w+1) + 4, (c.w+1) + 5]),
                (0, 2, ARRAY[4, 5, (c.w+1) + 4, (c.w+1) + 5]),
                (0, 3, ARRAY[4, 5, (c.w+1) + 4, (c.w+1) + 5]),
                -- I
                (1, 0, ARRAY[3, 4, 5, 6]),
                (1, 1, ARRAY[-(c.w+1) + 4, 4, 1*(c.w+1) + 4, 2*(c.w+1) + 4]),
                (1, 2, ARRAY[3, 4, 5, 6]),
                (1, 3, ARRAY[-(c.w+1) + 4, 4, 1*(c.w+1) + 4, 2*(c.w+1) + 4]),
                -- T
                (2, 0, ARRAY[3, 4, 5, (c.w+1) + 4]),
                (2, 1, ARRAY[-(c.w+1) + 4, 3, 4, (c.w+1) + 4]),
                (2, 2, ARRAY[-(c.w+1) + 4, 3, 4, 5]),
                (2, 3, ARRAY[-(c.w+1) + 4, 4, 5, (c.w+1) + 4]),
                -- L
                (3, 0, ARRAY[3, 4, 5, (c.w+1) + 3]),
                (3, 1, ARRAY[-(c.w+1) + 3, -(c.w+1) + 4, 4, (c.w+1) + 4]),
                (3, 2, ARRAY[-(c.w+1) + 5, 3, 4, 5]),
                (3, 3, ARRAY[-(c.w+1) + 4, 4, (c.w+1) + 4, (c.w+1) + 5]),
                -- J
                (4, 0, ARRAY[3, 4, 5, (c.w+1) + 5]),
                (4, 1, ARRAY[-(c.w+1) + 4, 4, (c.w+1) + 3, (c.w+1) + 4]),
                (4, 2, ARRAY[-(c.w+1) + 3, 3, 4, 5]),
                (4, 3, ARRAY[-(c.w+1) + 4, -(c.w+1) + 5, 4, (c.w+1) + 4]),
                -- S
                (5, 0, ARRAY[4, 5, (c.w+1) + 3, (c.w+1) + 4]),
                (5, 1, ARRAY[-(c.w+1) + 4, 4, 5, (c.w+1) + 5]),
                (5, 2, ARRAY[4, 5, (c.w+1) + 3, (c.w+1) + 4]),
                (5, 3, ARRAY[-(c.w+1) + 4, 4, 5, (c.w+1) + 5]),
                -- Z
                (6, 0, ARRAY[3, 4, (c.w+1) + 4, (c.w+1) + 5]),
                (6, 1, ARRAY[-(c.w+1) + 5, 4, 5, (c.w+1) + 4]),
                (6, 2, ARRAY[3, 4, (c.w+1) + 4, (c.w+1) + 5]),
                (6, 3, ARRAY[-(c.w+1) + 5, 4, 5, (c.w+1) + 4])
        ) _(id, rotation, piece)
    ),
    -- Connect to the local database with dblink once at the start of the query, to later read the
    -- the Input table. If the connection already exists, skips the creation.
    conn(name, _) AS (
        SELECT 'conn',
            CASE
                -- connection exists
                WHEN ARRAY['conn'] <@ dblink_get_connections() THEN ''
                -- connection does not exist
                ELSE dblink_connect('conn', 'dbname=' || current_database())
            END
    )
    -- Non-recursive term of the main loop, i.e., the initial state
    SELECT
        -- frame
        0 AS frame,
        -- board: boolean 1d array where each position states if a cell is occupied or not. in
        -- addition to the regular playable const.width cells in each line, there is a extra cell at
        -- the end that is always occupied, to allow the side limits to be determined in a 1d array.
        -- 1d arrays are used instead of 2d as they are easier to work with in Postgres.
        string_to_array(repeat(repeat('f', const.width) || 't', const.height), NULL)::bool[] AS board,
        -- score
        0 AS score,
        -- number of lines cleared
        0 AS lines,
        -- drop delta
        const.init_drop_delta AS drop_delta,
        -- position information, storing the piece id, the rotation, the number of cells it has
        -- moved (where 0 is the default position), and the piece status:
        --   1 - piece was dropped, either naturally or by user input, notifying that the piece
        --       might have reached the end
        --   2 - new piece spawn, notifying that the next piece needs to be generated
        --   0 - every other case, nothing to do
        (
            SELECT ARRAY[id, 0, 0, 0]
            FROM tetromino
            ORDER BY random()
            LIMIT 1
        ) AS pos,
        -- number of lines a piece can be dropped. is used to simulate where the piece is going to
        -- land, to allow hard drops, and to determine game over (max_drop_lines = -1)
        0 AS max_drop_lines,
        -- next piece to spawn, to allow next piece preview
        (
            SELECT id
            FROM tetromino
            ORDER BY random()
            LIMIT 1
        ) AS next_piece,
        -- last time a piece was dropped, either naturally or by user input. when the last_drop_time
        -- + drop_delta >= current time, the piece falls naturally. clock_timestamp() is used here
        -- and throughout the query since now() is transactional, i.e., reflects the time at the
        -- start of the query
        clock_timestamp() AS last_drop_time,
        -- last registered input time, to execute each input only once
        clock_timestamp() AS last_input_time,
        -- render
        notify('start'),
        -- sleep
        pg_sleep(0),
        -- last frame time, so the next sleep can be set in a way that matches the specified fps
        clock_timestamp() AS last_frame_time
        FROM const
    UNION ALL
    -- Recursive term, called at each frame.
    -- It starts by first reading the user input. Then, it processes the piece movement, updating
    -- the board, score, how far a piece can drop, and so on. Next, it renders the current state,
    -- using the notify function. Finally, it performs a sleep to match the specified fps.
    SELECT
        -- frame
        main.frame + 1,
        -- board
        next_board.board,
        -- score
        main.score + next_board.earned_points,
        -- number of lines cleared
        main.lines + next_board.lines_cleared,
        -- drop delta based on current level
        greatest(const.min_drop_delta,
                 const.init_drop_delta
                 - const.drop_delta_decrease * ((main.lines + next_board.lines_cleared) / const.lines_per_level)),
        -- piece position (set the last element to 0 to reset the piece status in the next frame)
        movement.pos[:3] || ARRAY[0],
        -- max drop lines
        drop_piece.lines,
        -- next piece id
        next_piece.id,
        -- last drop time
        movement.drop_time,
        -- last input time
        movement.input_time,
        -- render
        notify(render.string),
        -- sleep the required amount to match the fps. the longer the time it takes to compute a
        -- frame, the less it needs to sleep
        pg_sleep(extract(epoch FROM
                         main.last_frame_time + make_interval(secs => 1 / const.fps::decimal) - clock_timestamp())),
        -- last frame time
        clock_timestamp()
    FROM main,
        const,
        conn,
        -- retrieve the user input; the current frame is appended to the query to avoid it to be
        -- cached by the optimizer
        dblink(conn.name, 'SELECT * FROM Input --' || main.frame) input (cmd char, ts timestamp),
        -- compute the new position based on the user input. the LATERAL join allows each row of the
        -- previous relation (in this case, there is only one row) to be used inside the subquery
        LATERAL (
            -- next position of the piece, based on the user input / natural fall
            WITH next_pos(pos, drop_time, input_time) AS (
                -- check if its time for the piece to fall naturally
                WITH natural_fall(natural_fall) AS (
                    SELECT main.last_drop_time + make_interval(secs => main.drop_delta) <= clock_timestamp()
                        AND input.cmd <> 'p' AS natural_fall -- if paused, do not move
                )
                SELECT
                    -- position
                    CASE
                        -- natural fall, increase the position by one line
                        WHEN natural_fall THEN
                            main.pos[:2] || ARRAY[main.pos[3] + const.width + 1] || 1
                        -- user input
                        WHEN input.ts > main.last_input_time THEN
                            CASE
                                WHEN input.cmd = 'u' THEN main.pos[:1] || ARRAY[(main.pos[2] + 1) % 4] || main.pos[3:]
                                WHEN input.cmd = 'd' THEN main.pos[:2] || ARRAY[main.pos[3] + const.width + 1] || 1
                                WHEN input.cmd = 'l' THEN main.pos[:2] || ARRAY[main.pos[3] - 1] || main.pos[4]
                                WHEN input.cmd = 'r' THEN main.pos[:2] || ARRAY[main.pos[3] + 1] || main.pos[4]
                                WHEN input.cmd = 's' THEN
                                    main.pos[:2] || ARRAY[main.pos[3] + main.max_drop_lines * (const.width + 1)] || 1
                            END
                        -- nothing to do, position stays the same
                        ELSE
                            main.pos
                    END AS pos,
                    -- last_drop_time
                    CASE
                        -- piece moved
                        WHEN natural_fall OR (input.ts > main.last_input_time AND input.cmd = 'd') THEN
                            clock_timestamp()
                        -- when a piece is hard-dropped, ensure that there is a natural drop in the
                        -- next frame, to make the next piece appear faster
                        WHEN (input.ts > main.last_input_time AND input.cmd = 's') THEN
                            main.last_drop_time - make_interval(secs => main.drop_delta)
                        -- nothing to do
                        ELSE
                            main.last_drop_time
                    END AS drop_time,
                    -- last_input_time. only update it if the input was processed. this avoids the
                    -- input being skipped when the natural fall occurs in the same frame
                    CASE
                        WHEN NOT natural_fall THEN
                            input.ts
                        ELSE
                            main.last_input_time
                    END AS input_time
                    FROM natural_fall
            ),
            -- compute the new piece based on the next position
            piece_after_movement(new_piece) AS (
                SELECT array_agg(cell)::integer[] AS new_piece
                FROM (
                    SELECT unnest(piece) + next_pos.pos[3] AS cell
                    FROM tetromino, next_pos
                    WHERE id = next_pos.pos[1]
                        AND rotation = next_pos.pos[2]
                ) _
            -- check if the new piece collides with any filled cell in the board
            ), collision(collides) AS (
                SELECT bool_or(cell) AS collides
                FROM unnest(main.board) WITH ORDINALITY b(cell, ordinality)
                JOIN unnest((SELECT new_piece FROM piece_after_movement)) p(coord)
                    ON p.coord + 1 = b.ordinality
            )
            -- check if the next position is valid
            SELECT drop_time, input_time,
                CASE
                    -- new piece is in a valid place
                    WHEN
                        -- no block reached the end
                        (NOT new_piece && ARRAY(SELECT (const.width + 1) * const.height + i
                                                FROM generate_series(0, const.width + 1) _(i)))
                            -- no block in the -1 or in the -(width + 1) - 1 positions
                            AND (NOT new_piece && ARRAY[-1]) AND NOT (new_piece && ARRAY[-(const.width + 1) - 1])
                            -- no block clashes with filled cells in the board
                            AND (NOT collision.collides) THEN
                        next_pos.pos
                    -- new piece reached the end or it clashes with another block moving down ->
                    -- spawn a new piece
                    WHEN next_pos.pos[4] = 1
                        AND (
                            new_piece && ARRAY(SELECT (const.width + 1) * const.height + i
                                               FROM generate_series(0, const.width + 1) _(i))
                            OR collision.collides
                        ) THEN
                            ARRAY[main.next_piece, 0, 0, 2]
                    -- not a valid movement and did not reach the end, keep the same position
                    ELSE
                        main.pos
                END AS pos
            FROM next_pos, piece_after_movement, collision
        ) movement,
        -- update the board considering the movement
        LATERAL (
            -- board with the new blocks, if the current piece reached the end
            WITH new_board(board) AS (
                SELECT
                    CASE
                        -- a new piece is going to spawn, meaning the previous piece blocks can be
                        -- added to the board
                        WHEN movement.pos[4] = 2 THEN (
                            -- last piece, to add to the board
                            WITH RECURSIVE last_piece(piece) AS (
                                SELECT array_agg(cell)
                                FROM (
                                    SELECT unnest(piece) + main.pos[3] AS cell
                                    FROM tetromino
                                    WHERE id = main.pos[1]
                                        AND rotation = main.pos[2]
                                ) _
                            ),
                            -- since the board is immutable, each piece block must be incrementally
                            -- added to it, using a recursive query
                            board_with_piece(i, board) AS (
                                SELECT 1 AS i, main.board
                                UNION ALL
                                SELECT board_with_piece.i + 1,
                                    CASE
                                        -- block in the board
                                        WHEN piece[i] >= 0 THEN
                                            board_with_piece.board[:piece[i]] || '{t}'
                                            || board_with_piece.board[piece[i] + 2:]
                                        -- block coordinates are not in the board, skip. can happen
                                        -- when a piece is rotated while at the top
                                        ELSE
                                            board_with_piece.board
                                    END
                                FROM board_with_piece, last_piece
                                WHERE board_with_piece.i <= array_length(piece, 1)
                            )
                            -- retrieve the last materialization of the board
                            SELECT board
                            FROM board_with_piece
                            ORDER BY i DESC
                            LIMIT 1
                        )
                        -- the piece did not reach the end yet, keep the same board
                        ELSE
                            main.board
                    END AS board
            ),
            -- remove any completed lines from the new board
            new_board_compressed AS (
                -- aggregate back into a single array; count the number of remaining lines
                SELECT array_agg(cell ORDER BY line_number, col_number) AS board,
                    (count(*) / (const.width + 1))::int AS num_lines
                FROM (
                    -- filter out completed lines
                    SELECT line_number, generate_series(0, const.width) AS col_number, unnest(line) AS cell
                    FROM (
                        -- split into one board line per row
                        SELECT i AS line_number, board[i*(const.width + 1)+1:(i+1)*(const.width+1)] line
                        FROM new_board, generate_series(0, const.height - 1) _(i)
                    ) _
                    -- filter out lines that have only true values
                    WHERE NOT line <@ ARRAY[true]
                ) _
            )
            -- add new empty lines at the top of the board, if needed, and compute the number of
            -- lines cleared and points earned
            SELECT string_to_array(repeat(repeat('f', const.width) || 't', const.height - num_lines), NULL)::bool[]
                    || board AS board,
                const.height - num_lines AS lines_cleared,
                (
                    SELECT points *
                        (greatest(1, (main.lines / const.lines_per_level + 1) * const.level_score_multiplier))
                    FROM points_per_line
                    WHERE lines = const.height - num_lines
                ) AS earned_points
            FROM new_board_compressed
        ) next_board,
        -- find out how many lines can we drop the current piece
        LATERAL (
            WITH RECURSIVE curr_piece(piece) AS (
                SELECT piece
                FROM tetromino
                WHERE id = movement.pos[1]
                    AND rotation = movement.pos[2]
            ),
            -- move the piece line by line until it collides with a block or reaches the end.
            -- if the piece cannot move a single line, return -1
            t (lines) AS (
                SELECT -1
                UNION ALL
                SELECT lines + 1
                FROM t, curr_piece
                WHERE NOT (
                    SELECT bool_or(cell) OR bool_or(cell IS NULL)
                    FROM unnest(piece) p(coord)
                    -- left join with the board to check the validity of the piece blocks
                    -- (left and not inner since we also need to check piece blocks out of bounds)
                    LEFT JOIN unnest(next_board.board) WITH ORDINALITY b(cell, ordinality)
                        ON (p.coord + movement.pos[3]) + 1 + (lines + 1) * (const.width + 1) = b.ordinality
                    WHERE (p.coord + movement.pos[3]) + 1 + (lines + 1) * (const.width + 1) >= 1
                )
            )
            SELECT max(lines) AS lines
            FROM t
        ) drop_piece,
        -- generate the next piece (if necessary), using a similar algorithm to NES Tetris: first,
        -- a piece is randomly selected; if it is different from the previous one, it becomes the
        -- next piece; otherwise, we generate another random piece and use it as the next piece.
        -- this is biased to not select the same piece twice in a row, but can still happen (1/49)
        LATERAL (
            SELECT
                CASE
                    -- next piece needed
                    WHEN movement.pos[4] = 2 THEN (
                        SELECT id
                        FROM (
                            -- first piece roll, discard it if it matches the previous piece
                            SELECT id, 0 AS rank
                            FROM (
                                SELECT id
                                FROM tetromino
                                -- the current frame is added to avoid the query from being cached
                                ORDER BY random() + main.frame
                                LIMIT 1
                            ) _
                            WHERE id != movement.pos[1]
                            UNION ALL
                            -- second piece roll
                            (
                                SELECT id, 1 AS rank
                                FROM tetromino
                                -- the current frame is added to avoid the query from being cached
                                ORDER BY random() + main.frame
                                LIMIT 1
                            )
                        ) _
                        -- if we generated two valid pieces, select only the first one
                        ORDER BY rank
                        LIMIT 1
                    )
                    -- nothing to do
                    ELSE
                        main.next_piece
                END AS id
        ) next_piece,
        -- compute the string to render
        LATERAL (
            SELECT
                -- header
                E'\n\n' ||
                (CASE WHEN input.cmd = 'p' THEN 'PAUSED' ELSE '' END) ||
                E'\nScore: ' || (main.score + next_board.earned_points) ||
                ' / Lines: ' || (main.lines + next_board.lines_cleared) ||
                ' / Level: ' || ((main.lines + next_board.lines_cleared) / const.lines_per_level + 1) ||
                -- next piece indicator
                E'\nNext: ' || (
                    WITH RECURSIVE next_piece(piece) AS (
                        SELECT array_agg(cell)
                        FROM (
                            SELECT unnest(piece) - 3 AS cell
                            FROM tetromino
                            WHERE tetromino.id = next_piece.id
                                AND tetromino.rotation = 0
                        ) _
                    ),
                    next_piece_block(i, block) AS (
                        SELECT 1 AS i, string_to_array(repeat(repeat('f', const.width) || E'\n', 2), NULL) AS block
                        UNION ALL
                        SELECT i + 1, block[:piece[i]] || '{t}' || block[piece[i] + 2:]
                        FROM next_piece_block, next_piece
                        WHERE i <= array_length(piece, 1)
                    )
                    -- pretty print the next piece blocks, add extra spacing to align with the
                    -- 'Next:' label, and remove the extra newline
                    SELECT replace(replace(replace(
                                array_to_string(block[:array_length(block, 1) - 1], ''),
                                't', '[]'), 'f', '  '), E'\n', E'\n      ')
                    FROM next_piece_block
                    ORDER BY i DESC
                    LIMIT 1
                ) ||
                -- board
                E'\n+' || repeat('-', const.width * 2) || E'+\n' || (
                    -- materialize the current piece and the ghost_piece, i.e., where the current
                    -- piece is going to fall on the board
                    WITH RECURSIVE pieces(curr_piece, ghost_piece) AS (
                        SELECT array_agg(curr_cell),
                            array_agg(curr_cell + greatest(drop_piece.lines, 0) * (const.width + 1))
                        FROM (
                            SELECT unnest(piece) + movement.pos[3] AS curr_cell
                            FROM tetromino
                            WHERE id = movement.pos[1]
                                AND rotation = movement.pos[2]
                        ) _
                    ),
                    -- materialize the board + ghost piece (ghost blocks marked with the '.' char)
                    board_with_ghost_piece(i, board) AS (
                        SELECT 1 AS i, next_board.board::char[]
                        UNION ALL
                        SELECT i + 1,
                            CASE
                                WHEN ghost_piece[i] >= 0 THEN
                                    board[:ghost_piece[i]] || '{.}' || board[ghost_piece[i] + 2:]
                                ELSE
                                    board
                            END::char[] AS board
                        FROM board_with_ghost_piece, pieces
                        WHERE i <= array_length(curr_piece, 1)
                    ),
                    -- materialize the (board + ghost piece) + current piece
                    board_with_piece(i, board) AS (
                        SELECT 1, board
                        FROM (
                            SELECT board
                            FROM board_with_ghost_piece
                            ORDER BY i DESC
                            LIMIT 1
                        ) _
                        UNION ALL
                        SELECT i + 1,
                            CASE
                                WHEN curr_piece[i] >= 0 THEN
                                    board[:curr_piece[i]] || '{t}' || board[curr_piece[i] + 2:]
                                ELSE
                                    board
                            END::char[]
                        FROM board_with_piece, pieces
                        WHERE i <= array_length(curr_piece, 1)
                    ),
                    -- add borders to the board
                    complete_board AS (
                        SELECT (ordinality - 1) / (const.width + 1) AS line_number,
                            ARRAY['|']::char[] ||
                              (array_agg(cell ORDER BY ordinality))[:const.width] ||
                              ARRAY['|', E'\n']::char[] AS line
                        FROM (
                            SELECT *
                            FROM unnest((
                                SELECT board
                                FROM board_with_piece
                                ORDER BY i DESC
                                LIMIT 1
                            )) WITH ORDINALITY AS _(cell, ordinality)
                        ) _
                        GROUP BY 1
                    )
                    -- pretty print, converting 't' to '[]', '.' to '()', and 'f' to '  '
                    SELECT replace(replace(replace(
                            array_to_string(array_agg(line ORDER BY line_number), ''),
                            't', '[]'), '.', '()'), 'f', '  ')
                    FROM complete_board
                ) || '+' || repeat('-', const.width * 2) || '+' AS string
        ) render
    -- keep executing the main loop until the piece is not stuck at the start (-1)
    WHERE main.max_drop_lines >= 0
)
-- project only the maximum score at the end
SELECT 'score: ' || max(score) AS game_over
FROM main;
