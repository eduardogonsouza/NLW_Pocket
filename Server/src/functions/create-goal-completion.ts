import { count, gte, lte, and, eq, sql } from "drizzle-orm";
import { db } from "../db";
import { goalCompletions, goals } from "../db/schema";

import dayjs from "dayjs";

interface CreateGoalCompletionRequest {
	goalId: string;
}

export async function createGoalsCompletions({
	goalId,
}: CreateGoalCompletionRequest) {
	const firstDayForWeek = dayjs().startOf("week").toDate();
	const lastDayOfWeek = dayjs().endOf("week").toDate();

	const goalCompletionsCounts = db.$with("goal_completions_counts").as(
		db
			.select({
				goalId: goalCompletions.id,
				completionCount: count(goalCompletions.id).as("completionCount"),
			})
			.from(goalCompletions)
			.where(
				and(
					gte(goalCompletions.createdAt, firstDayForWeek),
					lte(goalCompletions.createdAt, lastDayOfWeek),
					eq(goals.id, goalId),
				),
			)
			.groupBy(goalCompletions.goalId),
	);

	const result = await db
		.with(goalCompletionsCounts)
		.select({
			desiredWeeklyFrequency: goals.desiredWeeklyFrequency,
			completionCount:
				sql`COALEASCE(${goalCompletionsCounts.completionCount}, 0)`.mapWith(
					Number,
				),
		})
		.from(goals)
		.leftJoin(goalCompletionsCounts, eq(goalCompletionsCounts.goalId, goalId))
		.where(eq(goals.id, goalId))
		.limit(1);

	const { completionCount, desiredWeeklyFrequency } = result[0];

	if (completionCount >= desiredWeeklyFrequency) {
		throw new Error("Goal already completed this week!");
	}
	const insertResult = await db
		.insert(goalCompletions)
		.values({
			goalId,
		})
		.returning();

	const goalCompletion = insertResult[0];

	return { goalCompletion };
}
