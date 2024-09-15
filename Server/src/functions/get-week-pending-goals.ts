import { db } from "../db";
import { goalCompletions, goals } from "../db/schema";
import { and, count, gte, lte, eq, sql } from "drizzle-orm";

import dayjs from "dayjs";

export async function getWeekPendingGoals() {
	const firstDayForWeek = dayjs().startOf("week").toDate();
	const lastDayOfWeek = dayjs().endOf("week").toDate();

	const goalsCreatedUpToWeek = db.$with("goals_created_up_to_week").as(
		db
			.select({
				id: goals.id,
				title: goals.title,
				desiredWeeklyFrequency: goals.desiredWeeklyFrequency,
				createAt: goals.createdAt,
			})
			.from(goals)
			.where(lte(goals.createdAt, lastDayOfWeek)),
	);

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
				),
			)
			.groupBy(goalCompletions.goalId),
	);

	const pendingGoals = await db
		.with(goalsCreatedUpToWeek, goalCompletionsCounts)
		.select({
			id: goalsCreatedUpToWeek.id,
			title: goalsCreatedUpToWeek.title,
			desiredWeeklyFrequency: goalsCreatedUpToWeek.desiredWeeklyFrequency,
			completionCount:
				sql`COALEASCE(${goalCompletionsCounts.completionCount}, 0)`.mapWith(
					Number,
				),
		})
		.from(goalsCreatedUpToWeek)
		.leftJoin(
			goalCompletionsCounts,
			eq(goalCompletionsCounts.goalId, goalsCreatedUpToWeek.id),
		);

	return {
		pendingGoals,
	};
}
