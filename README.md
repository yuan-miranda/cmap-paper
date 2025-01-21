## Coordinate Mapper PaperMc Plugin (cmap-paper)
Minecraft server-side plugin that tracks player coordinates (x, z) across all dimensions (overworld, nether, end) and send into a PostgreSQL database. Working alongside with [cmap](https://github.com/yuan-miranda/cmap) which generates and render the tiles to the website.

## Usage and Example Commands
| Command                                                    | Operation                                           |
|------------------------------------------------------------|-------------------------------------------          |
| /cm add nidianazareno                                      | add nidianazareno to coordinates tracking (cm=cmap) |
| /cmap remove nidianazareno                                 | remove nidianazareno                                |
| /cmap list                                                 | list all the tracked players                        |
| /cmap start                                                | stsrt the coordinate tracking                       |
| /cmap stop                                                 | stop the tracking                                   |
| /cmap reload                                               | reload the plugin                                   |
| /cmap dbconfig \<host> \<port> \<name> \<user> \<password> | setup the database connection                       |

## Note
[6f89bc5](https://github.com/yuan-miranda/cmap-paper/commit/6f89bc505e76614747adae16bf2a99aa4f12805f) Currently the plugin has no permission config meaning
everyone on the server can access the commands (add, remove, list, start, stop, reload, dbconfig). I dont intend to fix this yet but just keep that in mind.
for anyone who wants yo fix this issue, just create a pull request.
