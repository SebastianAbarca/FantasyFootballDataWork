# your_project/backend/schema.py

# Import Ariadne functions for schema and GraphQL execution
from ariadne import load_schema_from_path, make_executable_schema, QueryType, MutationType
from ariadne.types import Extension  # Can be removed if no extensions are used, but harmless to keep

# Import the db instance (using absolute import for robustness)
from backend import db

# Import all your SQLAlchemy models from the models package
# These models define the structure of your database tables in Python
from .models import (
    DimPlayers, DimTeams, PlayerYearlyStats, PlayerWeeklyStats,
    TeamWeeklyStats, TeamYearlyStats
)

# --- Load GraphQL Schema Definition Language (SDL) ---
# This line loads all .graphql files from the 'backend/graphql' directory.
# Ariadne combines them into a single schema definition.
type_defs = load_schema_from_path("backend/graphql")

# --- Define Query Resolvers ---
# QueryType binds Python functions to the Query fields defined in your .graphql schema.
query = QueryType()


@query.field("allPlayers")
def resolve_all_players(obj, info):
    """
    Resolver for the 'allPlayers' query.
    Fetches all records from the DimPlayers table.
    """
    return DimPlayers.query.all()


@query.field("player")
def resolve_player(obj, info, playerId):
    """
    Resolver for the 'player' query.
    Fetches a single DimPlayer record by its primary key (playerId).
    """
    return DimPlayers.query.get(playerId)


@query.field("allTeams")
def resolve_all_teams(obj, info):
    """
    Resolver for the 'allTeams' query.
    Fetches all records from the DimTeams table.
    """
    return DimTeams.query.all()


@query.field("team")
def resolve_team(obj, info, teamId):
    """
    Resolver for the 'team' query.
    Fetches a single DimTeam record by its primary key (teamId).
    """
    return DimTeams.query.get(teamId)


@query.field("allPlayerYearlyStats")
def resolve_all_player_yearly_stats(obj, info):
    """
    Resolver for the 'allPlayerYearlyStats' query.
    Fetches all records from the PlayerYearlyStats table.
    """
    return PlayerYearlyStats.query.all()


@query.field("playerYearlyStat")
def resolve_player_yearly_stat(obj, info, playerId, season, seasonType):
    """
    Resolver for the 'playerYearlyStat' query.
    Fetches a single PlayerYearlyStats record by its composite primary key.
    """
    return PlayerYearlyStats.query.get((playerId, season, seasonType))


@query.field("allPlayerWeeklyStats")
def resolve_all_player_weekly_stats(obj, info):
    """
    Resolver for the 'allPlayerWeeklyStats' query.
    Fetches all records from the PlayerWeeklyStats table.
    """
    return PlayerWeeklyStats.query.all()


@query.field("playerWeeklyStat")
def resolve_player_weekly_stat(obj, info, playerId, season, seasonType, week):
    """
    Resolver for the 'playerWeeklyStat' query.
    Fetches a single PlayerWeeklyStats record by its composite primary key.
    """
    return PlayerWeeklyStats.query.get((playerId, season, seasonType, week))


@query.field("allTeamWeeklyStats")
def resolve_all_team_weekly_stats(obj, info):
    """
    Resolver for the 'allTeamWeeklyStats' query.
    Fetches all records from the TeamWeeklyStats table.
    """
    return TeamWeeklyStats.query.all()


@query.field("teamWeeklyStat")
def resolve_team_weekly_stat(obj, info, gameId, teamId):
    """
    Resolver for the 'teamWeeklyStat' query.
    Fetches a single TeamWeeklyStats record by its composite primary key.
    """
    return TeamWeeklyStats.query.get((gameId, teamId))


@query.field("allTeamYearlyStats")
def resolve_all_team_yearly_stats(obj, info):
    """
    Resolver for the 'allTeamYearlyStats' query.
    Fetches all records from the TeamYearlyStats table.
    """
    return TeamYearlyStats.query.all()


@query.field("teamYearlyStat")
def resolve_team_yearly_stat(obj, info, teamId, season, seasonType):
    """
    Resolver for the 'teamYearlyStat' query.
    Fetches a single TeamYearlyStats record by its composite primary key.
    """
    return TeamYearlyStats.query.get((teamId, season, seasonType))


# --- Define Mutation Resolvers ---
# MutationType binds Python functions to the Mutation fields defined in your .graphql schema.
mutation = MutationType()


# --- DimPlayers Mutations ---
@mutation.field("createPlayer")
def resolve_create_player(obj, info, input: dict):  # Changed signature to accept 'input'
    """
    Resolver to create a new DimPlayer record.
    Checks for existing player ID to prevent duplicates.
    """
    player_id = input.get("playerId")  # Get playerId from the input dictionary
    if not player_id:  # Ensure playerId is provided in the input
        raise Exception("playerId is required for createPlayer mutation.")

    existing_player = DimPlayers.query.get(player_id)
    if existing_player:
        raise Exception("Player with this ID already exists.")

    # Convert GraphQL camelCase to SQLAlchemy snake_case for model fields
    # Exclude playerId as it's handled separately
    player_data = {k.replace('Id', '_id').replace('Name', '_name') if k.endswith('Id') or k.endswith('Name') else k for
                   k in input.keys()}  # Simplified conversion
    player_data = {key: value for key, value in input.items() if key != "playerId"}  # Extract data for model
    player_data_snake_case = {
        key.replace('Id', '_id').replace('Name', '_name')
        if key.endswith('Id') or key.endswith('Name')
        else key
        for key, value in player_data.items()
    }

    # Handle specific camelCase to snake_case conversions if needed
    converted_input = {}
    for k, v in input.items():
        if k == 'playerId':
            converted_input['player_id'] = v
        elif k == 'playerName':
            converted_input['player_name'] = v
        elif k == 'birthYear':
            converted_input['birth_year'] = v
        elif k == 'draftYear':
            converted_input['draft_year'] = v
        elif k == 'draftRound':
            converted_input['draft_round'] = v
        elif k == 'draftPick':
            converted_input['draft_pick'] = v
        elif k == 'draftOvr':
            converted_input['draft_ovr'] = v
        # Add other conversions for DimPlayer fields
        elif k == 'college':
            converted_input['college'] = v
        elif k == 'height':
            converted_input['height'] = v
        elif k == 'weight':
            converted_input['weight'] = v
        # Assuming teamId is still needed for other models' FKs but not on DimPlayers itself
        # If teamId is ever added back to DimPlayers:
        # elif k == 'teamId':
        #     converted_input['team_id'] = v
        else:
            converted_input[k] = v  # Keep as is if no conversion needed

    new_player = DimPlayers(**converted_input)
    db.session.add(new_player)
    db.session.commit()
    return new_player


@mutation.field("updatePlayer")
def resolve_update_player(obj, info, playerId, input: dict):  # Changed signature to accept 'input'
    """
    Resolver to update an existing DimPlayer record.
    """
    player = DimPlayers.query.get(playerId)
    if not player:
        raise Exception("Player not found!")

    converted_input = {}
    for k, v in input.items():
        if k == 'playerName':
            converted_input['player_name'] = v
        elif k == 'birthYear':
            converted_input['birth_year'] = v
        elif k == 'draftYear':
            converted_input['draft_year'] = v
        elif k == 'draftRound':
            converted_input['draft_round'] = v
        elif k == 'draftPick':
            converted_input['draft_pick'] = v
        elif k == 'draftOvr':
            converted_input['draft_ovr'] = v
        elif k == 'college':
            converted_input['college'] = v
        elif k == 'height':
            converted_input['height'] = v
        elif k == 'weight':
            converted_input['weight'] = v
        else:
            converted_input[k] = v

    for key, value in converted_input.items():
        setattr(player, key, value)
    db.session.commit()
    return player


@mutation.field("deletePlayer")
def resolve_delete_player(obj, info, playerId):
    """
    Resolver to delete a DimPlayer record.
    Returns True on successful deletion.
    """
    player = DimPlayers.query.get(playerId)
    if not player:
        raise Exception("Player not found!")
    db.session.delete(player)
    db.session.commit()
    return True


# --- DimTeams Mutations ---
@mutation.field("createTeam")
def resolve_create_team(obj, info, teamId, input: dict):  # Changed signature
    """
    Resolver to create a new DimTeam record.
    Checks for existing team ID to prevent duplicates.
    """
    if not teamId:
        raise Exception("teamId is required for createTeam mutation.")

    existing_team = DimTeams.query.get(teamId)
    if existing_team:
        raise Exception("Team with this ID already exists.")

    converted_input = {}
    for k, v in input.items():
        if k == 'teamName':
            converted_input['team_name'] = v
        else:
            converted_input[k] = v

    new_team = DimTeams(team_id=teamId, **converted_input)
    db.session.add(new_team)
    db.session.commit()
    return new_team


@mutation.field("updateTeam")
def resolve_update_team(obj, info, teamId, input: dict):  # Changed signature
    """
    Resolver to update an existing DimTeam record.
    """
    team = DimTeams.query.get(teamId)
    if not team:
        raise Exception("Team not found!")

    converted_input = {}
    for k, v in input.items():
        if k == 'teamName':
            converted_input['team_name'] = v
        else:
            converted_input[k] = v

    for key, value in converted_input.items():
        setattr(team, key, value)
    db.session.commit()
    return team


@mutation.field("deleteTeam")
def resolve_delete_team(obj, info, teamId):
    """
    Resolver to delete a DimTeam record.
    Returns True on successful deletion.
    """
    team = DimTeams.query.get(teamId)
    if not team:
        raise Exception("Team not found!")
    db.session.delete(team)
    db.session.commit()
    return True


# --- PlayerYearlyStats Mutations ---
@mutation.field("createPlayerYearlyStats")
def resolve_create_player_yearly_stats(obj, info, playerId, season, seasonType, input: dict):  # Changed signature
    """
    Resolver to create a new PlayerYearlyStats record.
    Checks for existing composite primary key to prevent duplicates.
    """
    existing_stats = PlayerYearlyStats.query.get((playerId, season, seasonType))
    if existing_stats:
        raise Exception("Player Yearly Stats entry already exists. Use update mutation.")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    new_stats = PlayerYearlyStats(player_id=playerId, season=season, season_type=seasonType, **converted_input)
    db.session.add(new_stats)
    db.session.commit()
    return new_stats


@mutation.field("updatePlayerYearlyStats")
def resolve_update_player_yearly_stats(obj, info, playerId, season, seasonType, input: dict):  # Changed signature
    """
    Resolver to update an existing PlayerYearlyStats record.
    """
    stats = PlayerYearlyStats.query.get((playerId, season, seasonType))
    if not stats:
        raise Exception("Player Yearly Stats entry not found!")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    for key, value in converted_input.items():
        setattr(stats, key, value)
    db.session.commit()
    return stats


@mutation.field("deletePlayerYearlyStats")
def resolve_delete_player_yearly_stats(obj, info, playerId, season, seasonType):
    """
    Resolver to delete a PlayerYearlyStats record.
    Returns True on successful deletion.
    """
    stats = PlayerYearlyStats.query.get((playerId, season, seasonType))
    if not stats:
        raise Exception("Player Yearly Stats entry not found!")
    db.session.delete(stats)
    db.session.commit()
    return True


# --- PlayerWeeklyStats Mutations ---
@mutation.field("createPlayerWeeklyStats")
def resolve_create_player_weekly_stats(obj, info, playerId, season, seasonType, week, input: dict):  # Changed signature
    """
    Resolver to create a new PlayerWeeklyStats record.
    Checks for existing composite primary key to prevent duplicates.
    """
    existing_stats = PlayerWeeklyStats.query.get((playerId, season, seasonType, week))
    if existing_stats:
        raise Exception("Player Weekly Stats entry already exists. Use update mutation.")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    new_stats = PlayerWeeklyStats(player_id=playerId, season=season, season_type=seasonType, week=week,
                                  **converted_input)
    db.session.add(new_stats)
    db.session.commit()
    return new_stats


@mutation.field("updatePlayerWeeklyStats")
def resolve_update_player_weekly_stats(obj, info, playerId, season, seasonType, week, input: dict):  # Changed signature
    """
    Resolver to update an existing PlayerWeeklyStats record.
    """
    stats = PlayerWeeklyStats.query.get((playerId, season, seasonType, week))
    if not stats:
        raise Exception("Player Weekly Stats entry not found!")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    for key, value in converted_input.items():
        setattr(stats, key, value)
    db.session.commit()
    return stats


@mutation.field("deletePlayerWeeklyStats")
def resolve_delete_player_weekly_stats(obj, info, playerId, season, seasonType, week):
    """
    Resolver to delete a PlayerWeeklyStats record.
    Returns True on successful deletion.
    """
    stats = PlayerWeeklyStats.query.get((playerId, season, seasonType, week))
    if not stats:
        raise Exception("Player Weekly Stats entry not found!")
    db.session.delete(stats)
    db.session.commit()
    return True


# --- TeamWeeklyStats Mutations ---
@mutation.field("createTeamWeeklyStats")
def resolve_create_team_weekly_stats(obj, info, gameId, teamId, input: dict):  # Changed signature
    """
    Resolver to create a new TeamWeeklyStats record.
    Checks for existing composite primary key to prevent duplicates.
    """
    existing_stats = TeamWeeklyStats.query.get((gameId, teamId))
    if existing_stats:
        raise Exception("Team Weekly Stats entry already exists. Use update mutation.")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    new_stats = TeamWeeklyStats(game_id=gameId, team_id=teamId, **converted_input)
    db.session.add(new_stats)
    db.session.commit()
    return new_stats


@mutation.field("updateTeamWeeklyStats")
def resolve_update_team_weekly_stats(obj, info, gameId, teamId, input: dict):  # Changed signature
    """
    Resolver to update an existing TeamWeeklyStats record.
    """
    stats = TeamWeeklyStats.query.get((gameId, teamId))
    if not stats:
        raise Exception("Team Weekly Stats entry not found!")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    for key, value in converted_input.items():
        setattr(stats, key, value)
    db.session.commit()
    return stats


@mutation.field("deleteTeamWeeklyStats")
def resolve_delete_team_weekly_stats(obj, info, gameId, teamId):
    """
    Resolver to delete a TeamWeeklyStats record.
    Returns True on successful deletion.
    """
    stats = TeamWeeklyStats.query.get((gameId, teamId))
    if not stats:
        raise Exception("Team Weekly Stats entry not found!")
    db.session.delete(stats)
    db.session.commit()
    return True


# --- TeamYearlyStats Mutations ---
@mutation.field("createTeamYearlyStats")
def resolve_create_team_yearly_stats(obj, info, teamId, season, seasonType, input: dict):  # Changed signature
    """
    Resolver to create a new TeamYearlyStats record.
    Checks for existing composite primary key to prevent duplicates.
    """
    existing_stats = TeamYearlyStats.query.get((teamId, season, seasonType))
    if existing_stats:
        raise Exception("Team Yearly Stats entry already exists. Use update mutation.")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    new_stats = TeamYearlyStats(team_id=teamId, season=season, season_type=seasonType, **converted_input)
    db.session.add(new_stats)
    db.session.commit()
    return new_stats


@mutation.field("updateTeamYearlyStats")
def resolve_update_team_yearly_stats(obj, info, teamId, season, seasonType, input: dict):  # Changed signature
    """
    Resolver to update an existing TeamYearlyStats record.
    """
    stats = TeamYearlyStats.query.get((teamId, season, seasonType))
    if not stats:
        raise Exception("Team Yearly Stats entry not found!")

    converted_input = convert_to_snake_case(input)  # Use helper for all fields
    for key, value in converted_input.items():
        setattr(stats, key, value)
    db.session.commit()
    return stats


@mutation.field("deleteTeamYearlyStats")
def resolve_delete_team_yearly_stats(obj, info, teamId, season, seasonType):
    """
    Resolver to delete a TeamYearlyStats record.
    Returns True on successful deletion.
    """
    stats = TeamYearlyStats.query.get((teamId, season, seasonType))
    if not stats:
        raise Exception("Team Yearly Stats entry not found!")
    db.session.delete(stats)
    db.session.commit()
    return True


# Helper function to convert GraphQL camelCase input to SQLAlchemy snake_case
def convert_to_snake_case(data: dict) -> dict:
    """
    Converts keys in a dictionary from camelCase to snake_case.
    Handles common conversions like 'Id' to '_id', 'Name' to '_name',
    and then applies general camelCase to snake_case.
    """
    snake_case_data = {}
    for key, value in data.items():
        # Handle specific common patterns first
        if key.endswith('Id'):
            new_key = key[:-2] + '_id'
        elif key.endswith('Name'):
            new_key = key[:-4] + '_name'
        elif key.endswith('Type'):  # For seasonType
            new_key = key[:-4] + '_type'
        elif key.endswith('Pct'):  # For offensePct, etc.
            new_key = key[:-3] + '_pct'
        elif key.endswith('Ovr'):  # For draftOvr
            new_key = key[:-3] + '_ovr'
        elif key.endswith('Pass'):  # For completePass, incompletePass
            new_key = key[:-4] + '_pass'
        elif key.endswith('Yards'):  # For passingYards, receivingYards
            new_key = key[:-5] + '_yards'
        elif key.endswith('Attempts'):  # For rushAttempts, passAttempts
            new_key = key[:-8] + '_attempts'
        elif key.endswith('Touchdown'):  # For rushTouchdown, passTouchdown
            new_key = key[:-9] + '_touchdown'
        elif key.endswith('Snaps'):  # For offenseSnaps, defenseSnaps
            new_key = key[:-5] + '_snaps'
        elif key.endswith('Hit'):  # For qbHit
            new_key = key[:-3] + '_hit'
        elif key.endswith('Conv'):  # For defensiveTwoPointConv
            new_key = key[:-4] + '_conv'
        elif key.endswith('Attempt'):  # For defensiveTwoPointAttempt
            new_key = key[:-7] + '_attempt'
        elif key.endswith('Points'):  # For totalOffPoints, totalDefPoints
            new_key = key[:-6] + '_points'
        elif key.endswith('Loss'):  # For totalOffPoints, totalDefPoints
            new_key = key[:-4] + '_loss'
        elif key.endswith('Conv'):  # For defensiveTwoPointConv
            new_key = key[:-4] + '_conv'
        elif key.endswith('Forced'):  # For fumbleForced
            new_key = key[:-6] + '_forced'
        elif key.endswith('Lost'):  # For fumbleLost
            new_key = key[:-4] + '_lost'
        elif key.endswith('OutOfBounds'):  # For fumbleOutOfBounds
            new_key = key[:-11] + '_out_of_bounds'
        elif key.endswith('Tackle'):  # For soloTackle, assistTackle
            new_key = key[:-6] + '_tackle'
        elif key.endswith('Assist'):  # For assistTackle
            new_key = key[:-6] + '_assist'
        elif key.endswith('WithAssist'):  # For tackleWithAssist
            new_key = key[:-10] + '_with_assist'
        elif key.endswith('Converted'):  # For thirdDownConverted
            new_key = key[:-9] + '_converted'
        elif key.endswith('Failed'):  # For thirdDownFailed
            new_key = key[:-6] + '_failed'
        elif key.endswith('Year'):  # For birthYear, draftYear
            new_key = key[:-4] + '_year'
        elif key.endswith('Round'):  # For draftRound
            new_key = key[:-5] + '_round'
        elif key.endswith('Pick'):  # For draftPick
            new_key = key[:-4] + '_pick'
        elif key.endswith('Win'):  # For homeWin
            new_key = key[:-3] + '_win'
        elif key.endswith('Tie'):  # For homeTie
            new_key = key[:-3] + '_tie'
        # General camelCase to snake_case conversion for remaining cases
        else:
            new_key = ''.join(['_' + c.lower() if c.isupper() else c for c in key]).lstrip('_')
        snake_case_data[new_key] = value
    return snake_case_data


# --- Create the executable GraphQL schema ---
# This combines your SDL type definitions with your Python resolvers.
schema = make_executable_schema(
    type_defs,  # The loaded schema definition language from .graphql files
    query,  # The QueryType with all your query resolvers
    mutation,  # The MutationType with all your your mutation resolvers
)