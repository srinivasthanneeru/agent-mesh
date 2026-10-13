// Drives the Claude Code CLI headlessly (`claude -p --output-format stream-json`)
// and translates its event stream into protocol chunks.
//
// One ClaudeSession = one conversation: the first turn records Claude's
// session_id and later turns pass --resume so the agent keeps its memory.

import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import type { Responder } from "../host.ts";

export interface ClaudeSessionOptions {
  cwd: string;
  model?: string;
  /** default | acceptEdits | bypassPermissions | plan */
  permissionMode?: string;
  /** e.g. ["Read","Glob","Grep"]; empty = CLI defaults */
  allowedTools?: string[];
  maxTurns?: number;
  bin?: string; // defaults to $CLAUDE_BIN or "claude"
}

export interface TurnResult {
  costUsd?: number;
  durationMs?: number;
  turns?: number;
  isError: boolean;
  /** True when the CLI failed before doing anything (e.g. --resume of an unknown session). */
  failedToStart?: boolean;
  errorText?: string;
}

// Env vars that make a child `claude` think it is nested inside another session.
const NESTING_ENV = /^(CLAUDECODE|CLAUDE_CODE_.*|CLAUDE_AGENT_SDK_.*|CLAUDE_PID|CLAUDE_EFFORT)$/;

export class ClaudeSession {
  sessionId: string | null = null;
  totalCostUsd = 0;
  turns = 0;
  private busy = false;

  readonly opts: ClaudeSessionOptions;

  constructor(opts: ClaudeSessionOptions) {
    this.opts = opts;
  }

  get isBusy(): boolean {
    return this.busy;
  }

  async run(prompt: string, res: Responder, signal: AbortSignal): Promise<TurnResult> {
    if (this.busy) throw Object.assign(new Error("session busy"), { code: 409 });
    this.busy = true;
    try {
      const resumed = this.sessionId !== null;
      let r = await this.exec(prompt, res, signal);
      // A conversation that can't be resumed (never saved, deleted, other
      // machine) fails before any turn runs: start a fresh one instead.
      if (resumed && r.failedToStart && !signal.aborted) {
        this.sessionId = null;
        res.raw("status_note", { message: "previous conversation unavailable; starting a new one" });
        r = await this.exec(prompt, res, signal);
      }
      if (r.failedToStart) res.send(`[claude error] ${r.errorText}`);
      return r;
    } finally {
      this.busy = false;
    }
  }

  private exec(prompt: string, res: Responder, signal: AbortSignal): Promise<TurnResult> {
    const o = this.opts;
    const args = ["-p", prompt, "--output-format", "stream-json", "--verbose", "--include-partial-messages"];
    if (this.sessionId) args.push("--resume", this.sessionId);
    if (o.model) args.push("--model", o.model);
    if (o.permissionMode) args.push("--permission-mode", o.permissionMode);
    if (o.allowedTools?.length) args.push("--allowedTools", o.allowedTools.join(","));
    if (o.maxTurns) args.push("--max-turns", String(o.maxTurns));

    const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !NESTING_ENV.test(k)));
    const child = spawn(o.bin ?? process.env.CLAUDE_BIN ?? "claude", args, {
      cwd: o.cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const kill = () => child.kill("SIGTERM");
    signal.addEventListener("abort", kill, { once: true });

    let stderr = "";
    child.stderr.on("data", (d) => (stderr = (stderr + d).slice(-4000)));

    return new Promise((resolve, reject) => {
      const result: TurnResult = { isError: false };
      let sawDelta = false;
      let sentText = "";
      let pendingSession: string | null = null;
      const rl = createInterface({ input: child.stdout });

      // The CLI retries silently forever on some failures (e.g. an expired
      // login), so give up if the model never starts answering. Only guards
      // the start of a turn: long tool runs after that are fine.
      const firstOutputS = Number(process.env.CLAUDE_FIRST_OUTPUT_S ?? 90);
      let stalled = false;
      const watchdog = setTimeout(() => {
        stalled = true;
        child.kill("SIGTERM");
      }, firstOutputS * 1000);

      rl.on("line", (line) => {
        let ev: any;
        try {
          ev = JSON.parse(line);
        } catch {
          return;
        }
        if (ev.type !== "system") clearTimeout(watchdog);
        switch (ev.type) {
          case "system":
            // Only adopted once the turn succeeds; a failed turn may never be saved.
            if (ev.subtype === "init" && ev.session_id) pendingSession = ev.session_id;
            break;
          case "stream_event": {
            // Token-level text streaming.
            const e = ev.event;
            if (e?.type === "content_block_delta" && e.delta?.type === "text_delta") {
              sawDelta = true;
              sentText += e.delta.text;
              res.send(e.delta.text);
            }
            if (e?.type === "message_start") sawDelta = false;
            break;
          }
          case "assistant":
            for (const block of ev.message?.content ?? []) {
              if (block.type === "tool_use") {
                // Non-standard chunk type: protocol callers that don't know it skip it (§6.6).
                res.raw("tool", { name: block.name, input: summarize(block.input) });
              } else if (block.type === "text" && !sawDelta && block.text) {
                sentText += block.text;
                res.send(block.text); // CLI without partial-message support; also synthetic error messages
              }
            }
            break;
          case "result":
            result.isError = !!ev.is_error;
            result.costUsd = ev.total_cost_usd;
            result.durationMs = ev.duration_ms;
            result.turns = ev.num_turns;
            if (typeof ev.total_cost_usd === "number") this.totalCostUsd += ev.total_cost_usd;
            if (!ev.is_error) {
              this.sessionId = ev.session_id ?? pendingSession ?? this.sessionId;
              break;
            }
            // Never end an errored turn silently: fall back to the subtype + stderr.
            result.errorText = ev.result || ev.errors?.join("; ") || lastLine(stderr) || ev.subtype || "unknown error";
            if (ev.num_turns === 0 && !sentText && ev.subtype === "error_during_execution") {
              result.failedToStart = true; // run() retries or reports it
            } else if (!sentText.includes(result.errorText!)) {
              res.send(`${sentText ? "\n" : ""}[claude error] ${result.errorText}`);
            }
            break;
        }
      });

      child.on("error", (e) => {
        clearTimeout(watchdog);
        signal.removeEventListener("abort", kill);
        reject(new Error(`cannot start claude CLI: ${e.message}`));
      });
      child.on("close", (code) => {
        clearTimeout(watchdog);
        signal.removeEventListener("abort", kill);
        this.turns++;
        if (stalled) {
          reject(
            new Error(
              `claude produced no response within ${firstOutputS}s. Usually its login has expired: ` +
                "run `claude` in a terminal and use /login, or test with `claude -p hi --debug`",
            ),
          );
        } else if (code !== 0 && !signal.aborted && result.costUsd === undefined) {
          reject(new Error(`claude exited ${code}: ${stderr.trim().split("\n").pop() ?? ""}`));
        } else resolve(result);
      });
    });
  }
}

function lastLine(s: string): string {
  return s.trim().split("\n").pop() ?? "";
}

function summarize(input: unknown): unknown {
  if (!input || typeof input !== "object") return input;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(input)) out[k] = typeof v === "string" && v.length > 200 ? v.slice(0, 200) + "…" : v;
  return out;
}

/** One-shot, tool-less completion via the CLI (used for routing/synthesis). */
export function claudeText(prompt: string, opts: { model?: string; bin?: string; timeoutMs?: number } = {}): Promise<string> {
  const args = ["-p", prompt, "--output-format", "json", "--tools", ""];
  if (opts.model) args.push("--model", opts.model);
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !NESTING_ENV.test(k)));
  return new Promise((resolve, reject) => {
    const child = spawn(opts.bin ?? process.env.CLAUDE_BIN ?? "claude", args, { env, stdio: ["ignore", "pipe", "pipe"] });
    const timer = setTimeout(() => child.kill("SIGTERM"), opts.timeoutMs ?? 120_000);
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    child.on("error", (e) => reject(e));
    child.on("close", (code) => {
      clearTimeout(timer);
      try {
        const j = JSON.parse(out);
        if (j.is_error) return reject(new Error(String(j.result)));
        resolve(String(j.result ?? ""));
      } catch {
        reject(new Error(`claude exited ${code}: ${(err || out).trim().slice(-300)}`));
      }
    });
  });
}
