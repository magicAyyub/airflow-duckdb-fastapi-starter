import * as React from "react"
import { cn } from "@/lib/utils"

interface CustomProgressProps extends React.HTMLAttributes<HTMLDivElement> {
  value?: number
  max?: number
  className?: string
  indicatorClassName?: string
}

const CustomProgress = React.forwardRef<HTMLDivElement, CustomProgressProps>(
  ({ value = 0, max = 100, className, indicatorClassName, ...props }, ref) => {
    const percentage = Math.min(Math.max(0, (value / max) * 100), 100)

    return (
      <div
        ref={ref}
        className={cn("relative h-2 w-full overflow-hidden rounded-full bg-gray-200 dark:bg-gray-700", className)}
        {...props}
      >
        <div
          className={cn("h-full w-full flex-1 bg-primary transition-all duration-300", indicatorClassName)}
          style={{ transform: `translateX(-${100 - percentage}%)` }}
        />
      </div>
    )
  },
)

CustomProgress.displayName = "CustomProgress"

export { CustomProgress }