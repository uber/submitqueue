"use client";

import { ChangeSubmissions, type ChangeDetailModel } from "@submitqueue/web-submitqueue";
import { useRouter } from "next/navigation";

export function ChangeView({ model }: { model: ChangeDetailModel }) {
  const router = useRouter();
  return <ChangeSubmissions model={model} onVersionChange={href => router.push(href)} />;
}
