import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { joinSession } from "@github/copilot-sdk/extension";

const extensionDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(extensionDir, "../../..");

function pythonExecutable() {
    if (process.env.DAX_QUERY_PACK_PYTHON) {
        return process.env.DAX_QUERY_PACK_PYTHON;
    }
    const localVenv = join(repoRoot, ".venv", "Scripts", "python.exe");
    if (process.platform === "win32" && existsSync(localVenv)) {
        return localVenv;
    }
    return process.platform === "win32" ? "python" : "python3";
}

function runPackCli(args) {
    const executable = pythonExecutable();
    const cliArgs = ["-m", "dax_query_mcp.query_pack_cli", ...args];
    return new Promise((resolveResult) => {
        execFile(
            executable,
            cliArgs,
            { cwd: repoRoot, windowsHide: true, maxBuffer: 10 * 1024 * 1024 },
            (error, stdout, stderr) => {
                const text = stdout.trim();
                let payload;
                try {
                    payload = text ? JSON.parse(text) : undefined;
                } catch {
                    payload = undefined;
                }

                if (payload?.ok === true) {
                    resolveResult(JSON.stringify(payload.data, null, 2));
                    return;
                }

                const failure = payload ?? {
                    ok: false,
                    error: {
                        type: error?.name ?? "CommandError",
                        message: stderr.trim() || error?.message || "dax-query-pack command failed",
                    },
                };
                resolveResult({
                    resultType: "failure",
                    textResultForLlm: JSON.stringify(failure, null, 2),
                });
            },
        );
    });
}

function addOptional(args, option, value) {
    if (value !== undefined && value !== null && value !== "") {
        args.push(option, String(value));
    }
}

function addRepeated(args, option, values) {
    for (const value of values ?? []) {
        if (value !== undefined && value !== null && value !== "") {
            args.push(option, String(value));
        }
    }
}

function daxPackContext(cwd) {
    if (!cwd) {
        return undefined;
    }
    const packPath = join(cwd, "pack.yaml");
    const queriesPath = join(cwd, "queries");
    if (!existsSync(packPath) && !existsSync(queriesPath)) {
        return undefined;
    }
    return [
        "This workspace appears to contain a DAX query pack.",
        "Prefer the dax_pack_* extension tools for reusable DAX workflows:",
        "create/add/validate/export packs, then use dax_pack_run or dax_pack_open_streamlit to return runnable commands.",
        "Do not store secrets in pack.yaml, connections.json, or generated artifacts.",
    ].join(" ");
}

await joinSession({
    tools: [
        {
            name: "dax_pack_create",
            description: "Create an empty durable DAX query pack with pack.yaml.",
            parameters: {
                type: "object",
                properties: {
                    output_dir: { type: "string", description: "Folder where pack.yaml should be created." },
                    name: { type: "string", description: "Human-readable pack name.", default: "query-pack" },
                    description: { type: "string", description: "Pack description.", default: "" },
                    overwrite: { type: "boolean", description: "Overwrite an existing manifest.", default: false },
                },
                required: ["output_dir"],
            },
            handler: async (args) => {
                const cliArgs = ["create", "--output-dir", args.output_dir];
                addOptional(cliArgs, "--name", args.name);
                addOptional(cliArgs, "--description", args.description);
                if (args.overwrite) cliArgs.push("--overwrite");
                return await runPackCli(cliArgs);
            },
        },
        {
            name: "dax_pack_add_query",
            description: "Add or update a DAX query in an existing query pack.",
            parameters: {
                type: "object",
                properties: {
                    pack_path: { type: "string", description: "Path to pack.yaml or the pack folder." },
                    connection_name: { type: "string", description: "Named DAX connection for this query." },
                    query: { type: "string", description: "DAX query text to save." },
                    description: { type: "string", description: "Query description." },
                    query_id: { type: "string", description: "Stable query ID slug.", default: "" },
                    display_name: { type: "string", description: "Human-friendly query name.", default: "" },
                    tags: { type: "string", description: "Comma-delimited tags.", default: "" },
                    parameters_json: {
                        type: "string",
                        description: "JSON object of parameter definitions.",
                        default: "",
                    },
                    table_name: { type: "string", description: "Default output table name.", default: "" },
                    overwrite: { type: "boolean", description: "Replace an existing query ID.", default: false },
                },
                required: ["pack_path", "connection_name", "query", "description"],
            },
            handler: async (args) => {
                const cliArgs = [
                    "add-query",
                    "--pack-path",
                    args.pack_path,
                    "--connection-name",
                    args.connection_name,
                    "--query",
                    args.query,
                    "--description",
                    args.description,
                ];
                addOptional(cliArgs, "--query-id", args.query_id);
                addOptional(cliArgs, "--display-name", args.display_name);
                addOptional(cliArgs, "--tags", args.tags);
                addOptional(cliArgs, "--parameters-json", args.parameters_json);
                addOptional(cliArgs, "--table-name", args.table_name);
                if (args.overwrite) cliArgs.push("--overwrite");
                return await runPackCli(cliArgs);
            },
        },
        {
            name: "dax_pack_validate",
            description: "Validate a DAX query pack against known connections and safe query-pack rules.",
            parameters: {
                type: "object",
                properties: {
                    pack_path: { type: "string", description: "Path to pack.yaml or the pack folder." },
                    connections_dir: { type: "string", description: "Connections directory.", default: "" },
                },
                required: ["pack_path"],
            },
            handler: async (args) => {
                const cliArgs = ["validate", "--pack-path", args.pack_path];
                addOptional(cliArgs, "--connections-dir", args.connections_dir);
                return await runPackCli(cliArgs);
            },
        },
        {
            name: "dax_pack_export",
            description: "Export a DAX query pack as a runnable Python, Streamlit, and Power Query workspace.",
            parameters: {
                type: "object",
                properties: {
                    pack_path: { type: "string", description: "Path to pack.yaml or the pack folder." },
                    output_dir: { type: "string", description: "Destination workspace folder.", default: "" },
                    connections_dir: { type: "string", description: "Connections directory.", default: "" },
                    include_power_query: { type: "boolean", description: "Generate power_query/*.pq.", default: true },
                    include_streamlit: { type: "boolean", description: "Generate streamlit_app.py.", default: true },
                    overwrite: { type: "boolean", description: "Overwrite copied pack files.", default: false },
                },
                required: ["pack_path"],
            },
            handler: async (args) => {
                const cliArgs = ["export", "--pack-path", args.pack_path];
                addOptional(cliArgs, "--output-dir", args.output_dir);
                addOptional(cliArgs, "--connections-dir", args.connections_dir);
                if (args.include_power_query === false) cliArgs.push("--no-power-query");
                if (args.include_streamlit === false) cliArgs.push("--no-streamlit");
                if (args.overwrite) cliArgs.push("--overwrite");
                return await runPackCli(cliArgs);
            },
        },
        {
            name: "dax_pack_run",
            description: "Return the shell command to run exported query-pack queries with uv.",
            parameters: {
                type: "object",
                properties: {
                    workspace_dir: { type: "string", description: "Exported query-pack workspace folder." },
                    only: { type: "array", items: { type: "string" }, description: "Query IDs to run.", default: [] },
                    tag: { type: "array", items: { type: "string" }, description: "Tags to include.", default: [] },
                    param: {
                        type: "array",
                        items: { type: "string" },
                        description: "Parameter overrides as name=value.",
                        default: [],
                    },
                    output: { type: "string", description: "Output folder.", default: "" },
                    format: { type: "string", enum: ["", "csv", "json"], description: "Output format.", default: "" },
                    max_rows: { type: "number", description: "Optional row cap." },
                    continue_on_error: { type: "boolean", description: "Continue after query errors.", default: false },
                    fail_fast: { type: "boolean", description: "Stop on first error.", default: false },
                },
                required: ["workspace_dir"],
            },
            handler: async (args) => {
                const cliArgs = ["run-command", "--workspace-dir", args.workspace_dir];
                addRepeated(cliArgs, "--only", args.only);
                addRepeated(cliArgs, "--tag", args.tag);
                addRepeated(cliArgs, "--param", args.param);
                addOptional(cliArgs, "--output", args.output);
                addOptional(cliArgs, "--format", args.format);
                addOptional(cliArgs, "--max-rows", args.max_rows);
                if (args.continue_on_error) cliArgs.push("--continue-on-error");
                if (args.fail_fast) cliArgs.push("--fail-fast");
                return await runPackCli(cliArgs);
            },
        },
        {
            name: "dax_pack_open_streamlit",
            description: "Return the shell command to open the exported query-pack Streamlit explorer.",
            parameters: {
                type: "object",
                properties: {
                    workspace_dir: { type: "string", description: "Exported query-pack workspace folder." },
                },
                required: ["workspace_dir"],
            },
            handler: async (args) => runPackCli(["streamlit-command", "--workspace-dir", args.workspace_dir]),
        },
    ],
    hooks: {
        onUserPromptSubmitted: async (input) => {
            const additionalContext = daxPackContext(input.cwd);
            return additionalContext ? { additionalContext } : undefined;
        },
    },
});
