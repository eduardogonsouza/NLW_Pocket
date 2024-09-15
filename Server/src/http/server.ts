import { fastify } from "fastify";
import {
	serializerCompiler,
	validatorCompiler,
	type ZodTypeProvider,
} from "fastify-type-provider-zod";

import { createGoals } from "../functions/create-goal";
import { getWeekPendingGoals } from "../functions/get-week-pending-goals";
import { createGoalsCompletions } from "../functions/create-goal-completion";

import z from "zod";

const app = fastify().withTypeProvider<ZodTypeProvider>();

app.setValidatorCompiler(validatorCompiler);
app.setSerializerCompiler(serializerCompiler);

app.get("/pending-goals", async () => {
	const { pendingGoals } = await getWeekPendingGoals();
	return { pendingGoals };
});

app.post(
	"/completions",
	{
		schema: {
			body: z.object({
				goalId: z.string(),
			}),
		},
	},
	async (request) => {
		const { goalId } = request.body;

		await createGoalsCompletions({
			goalId,
		});
	},
);

app.post(
	"/goals",
	{
		schema: {
			body: z.object({
				title: z.string(),
				desiredWeeklyFrequency: z.number().int().min(1).max(7),
			}),
		},
	},
	async (request) => {
		const { title, desiredWeeklyFrequency } = request.body;

		await createGoals({
			title,
			desiredWeeklyFrequency,
		});
	},
);

app
	.listen({
		port: 3333,
	})
	.then(() => {
		console.log("HTTP server Running!");
	});
